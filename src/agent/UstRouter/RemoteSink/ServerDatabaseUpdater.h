/*
 *  Phusion Passenger - https://www.phusionpassenger.com/
 *  Copyright (c) 2016 Phusion Holding B.V.
 *
 *  "Passenger", "Phusion Passenger" and "Union Station" are registered
 *  trademarks of Phusion Holding B.V.
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */
#ifndef _PASSENGER_UST_ROUTER_REMOTE_SINK_SERVER_DATABASE_H_
#define _PASSENGER_UST_ROUTER_REMOTE_SINK_SERVER_DATABASE_H_

#include <boost/thread.hpp>
#include <vector>

#include <DataStructures/StringKeyTable.h>
#include <UstRouter/RemoteSink/Server.h>

namespace Passenger {
namespace UstRouter {
namespace RemoteSink {

using namespace std;
using namespace boost;


class ServerDatabase {
public:
	class Observer {
		virtual ~Observer() { }
		virtual serverDefinitionCheckedOut(const vector< pair<ServerDefinitionPtr *, void *> > &result) = 0;
	};

private:
	typedef boost::container::small_vector<4, ServerPtr> SmallServerList;
	typedef vector<ServerPtr> ServerList;

	struct KeyInfo {
		string groupId;
		unsigned long long lastRejectionErrorTime;
		unsigned long long recheckTimeoutWhenAllHealthy;
		unsigned long long recheckTimeoutWhenHaveErrors;

		KeyInfo()
			: lastRejectionErrorTime(0),
			  recheckTimeoutWhenAllHealthy(5 * 60 * 1000000),
			  recheckTimeoutWhenHaveErrors(60 * 1000000)
			{ }
	};

	struct Group {
		SmallServerList servers;
		SmallServerList balancingList;
	};

	boost::mutex syncher;
	StringKeyTable<KeyInfo> keys;
	StringKeyTable<Group> groups;
	StringKeyTable<bool> queue;


	void threadMain() {
		boost::unique_lock<boost::mutex> l(syncher);
		while (true) {
			calculateNextCheckupAndWakeTime();
			waitUntilNextCheckupTimeOrWokenUp(l);
			if (timeForNextCheckup()) {
				queryLoadBalancingStatusForAllKeys();
			}
			processCheckoutsInProgress();
		}
	}

	unsigned long long recheckServers(boost::unique_lock<boost::mutex> &l) {
		l.unlock();

		P_INFO("Rechecking Union Station gateway servers...");

		unsigned long long nextCheckupTime;
		unsigned int httpCode;
		string body;
		bool result;

		result = fetchLoadBalancingManifest(httpCode, body);
		if (result) {
			nextCheckupTime = handleManifest(l, httpCode, body);
		} else {
			nextCheckupTime = handleManifestDown();
			l.lock();
		}

		return nextCheckupTime;
	}

	unsigned long long handleManifest(boost::unique_lock<boost::mutex> &l,
		unsigned int httpCode, const string &data)
	{
		Json::Reader reader;
		Json::Value manifest;

		if (OXT_UNLIKELY(!reader.parse(data, manifest, false))) {
			l.lock();
			return handleManifestParseError(httpCode, data,
				reader.getFormattedErrorMessages());
		}
		if (OXT_UNLIKELY(!validateManifest(manifest))) {
			l.lock();
			return handleManifestInvalid(httpCode, data);
		}
		if (OXT_UNLIKELY(manifest["status"].asString() != "ok")) {
			l.lock();
			return handleManifestErrorMessage(manifest);
		}
		if (OXT_UNLIKELY(httpCode != 200)) {
			l.lock();
			return handleManifestInvalidHttpCode(manifest, httpCode);
		}

		vector<ServerDefinitionPtr> newServers;
		Json::Value::const_iterator it, end = manifest["targets"].begin();
		bool allServersUp = true;

		for (it = manifest["targets"].begin(); it != end; it++) {
			const Json::Value &target = *it;
			ServerDefinitionPtr serverDef = make_shared<ServerDefinition>(
				target["base_url"].asString(),
				target["weight"].asUInt()
			);

			allServersUp = ping(serverDef) && allServersUp;

			newServers.push_back(boost::move(serverDef));
		}

		l.lock();

		clearManifestErrorState();
		updateServerList(servers, newServers);
		upServers = indexServersByWeight(servers);

		if (manifest.isMember("retry_in")) {
			recheckTimeoutWhenAllHealthy =
				manifest["retry_in"]["all_healthy"].asUInt64() * 1000000;
			recheckTimeoutWhenHaveErrors =
				manifest["retry_in"]["has_errors"].asUInt64() * 1000000;
		}

		unsigned long long now = SystemTime::getUsec();
		if (allServersUp) {
			return now + recheckTimeoutWhenAllHealthy;
		} else {
			return now + recheckTimeoutWhenHaveErrors;
		}
	}

	unsigned long long handleManifestParseError(unsigned int httpCode,
		const string &data, const string &parseErrorMessage)
	{
		setManifestErrorState("Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + manifestURL
			+ " returned an invalid response (unparseable). "
			+ "Parse error: " + parseErrorMessage
			+ "; HTTP code: " + toString(httpCode)
			+ "; data: \"" + cEscapeString(data) + "\""));
		return SystemTime::getUsec() + recheckTimeoutWhenHaveErrors;
	}

	// Warning: we don't have the lock inside this function.
	bool validateManifest(const Json::Value &manifest) const {
		if (OXT_UNLIKELY(!manifest.isObject())) {
			return false;
		}
		if (OXT_UNLIKELY(manifest.isMember("status"))) {
			return false;
		}
		if (OXT_LIKELY(manifest["status"].asString() == "ok")) {
			if (OXT_UNLIKELY(!manifest.isMember("targets") || !manifest["targets"].isArray())) {
				return false;
			}

			Json::Value::const_iterator it, end = manifest["targets"].begin();
			for (it = manifest["targets"].begin(); it != end; it++) {
				const Json::Value &target = *it;
				if (OXT_UNLIKELY(!target.isObject())) {
					return false;
				}
				if (OXT_UNLIKELY(!target.isMember("base_url") || !target["base_url"].isString())) {
					return false;
				}
				if (OXT_UNLIKELY(!target.isMember("weight") || !target["weight"].isUInt())) {
					return false;
				}
				if (target["weight"].asUInt() == 0) {
					return false;
				}
			}
		} else if (manifest["status"].asString() == "error") {
			if (!manifest.isMember("message") || !manifest["message"].isString()) {
				return false;
			}
			if (manifest.isMember("retry_in") && !manifest["retry_in"].isUInt()) {
				return false;
			}
			if (manifest.isMember("error_id") && !manifest["error_id"].isString()) {
				return false;
			}
		} else {
			return false;
		}

		return true;
	}

	unsigned long long handleManifestInvalid(unsigned int &httpCode, const string &data) {
		setManifestErrorState("Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + manifestURL
			+ " returned a invalid response (parseable, but does not comply to expected structure). "
			+ "; HTTP code: " + toString(httpCode)
			+ "; data: \"" + cEscapeString(data) + "\""));
		return SystemTime::getUsec() + recheckTimeoutWhenHaveErrors;
	}

	unsigned long long handleManifestErrorMessage(const Json::Value &manifest) {
		P_ASSERT_EQ(manifest["status"].asString(), "error");

		string message = "Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + manifestURL
			+ " returned an error. Message from server: " + manifest["message"].asString();
		if (manifest.isMember("error_id")) {
			message.append("; error ID: ");
			message.append(manifest["error_id"].asString());
		}
		setManifestErrorState(message);

		if (manifest.isMember("retry_in")) {
			recheckTimeoutWhenHaveErrors = manifest["retry_in"].asUInt64() * 1000000;
		}

		return SystemTime::getUsec() + recheckTimeoutWhenHaveErrors;
	}

	unsigned long long handleManifestInvalidHttpCode(const Json::Value &manifest,
		unsigned int httpCode)
	{
		setManifestErrorState("Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + manifestURL
			+ " returned a invalid response. "
			+ "; HTTP code: " + toString(httpCode)
			+ "; data: \"" + cEscapeString(data) + "\""));
		return SystemTime::getUsec() + recheckTimeoutWhenHaveErrors;
	}

	void processCheckoutsInProgress(boost::unique_lock<boost::mutex> &l) {
		vector< pair<ServerDefinitionPtr, void *> > result;

		if (allDown) {
			foreach () {
				result.push_back(make_pair(ServerDefinitionPtr(), userdata));
			}
		} else {
			foreach () {
				unsigned int i = nextIndexToReturn;
				nextIndexToReturn = (nextIndexToReturn + 1) % upServers.size();
				result.push_back(make_pair(upServers[i], userdata));
			}
		}
		checkoutsInProgress.clear();

		l.unlock();
		if (!result.empty()) {
			observer->serverDefinitionCheckedOut(result);
		}
		l.lock();
	}

protected:
	// Marked virtual so that unit tests can stub this.
	virtual bool fetchLoadBalancingManifest(unsigned int &httpCode, string &body) {
		// TODO
		return true;
	}

	// Marked virtual so that unit tests can stub this.
	// Warning: we don't have the lock inside this function.
	bool ping(const ServerDefinitionPtr &serverDef) const {
		// TODO
	}

public:
	struct GroupIdsToKeysLookup {
		GetGroupIdsForKeysCallback callback;
		void *userData;
	};

	bool getGroupIdsForKeys(const HashedStaticString keys[], unsigned int count,
		GetGroupIdsForKeysCallback callback, void *userData)
	{
		boost::lock_guard<boost::mutex> l(syncher);
		for (unsigned int i = 0; i < count; i++) {
			KeyInfo *keyInfo;

			if (keys.lookup(key, &keyInfo)) {
				groupIds.push_back(keyInfo->groupId);
			} else {
				break;
			}
		}
		if (groupIds.size() == count) {
			return true;
		} else {
			for (unsigned int = 0; i < count; i++) {
				queuedGroupIdToKeyLookups.insert(keys[i], true, false);
			}
			queuedGroupIdToKeyLookupCallbacks.push_back(keys);
			wakeupEventLoop();
			return false;
		}
	}

	ServerPtr getNextUpServer(const StaticString &groupId) {
		boost::lock_guard<boost::mutex> l(syncher);
		KeyInfo *keyInfo;

		if (keys.lookup(key, &keyInfo)) {
			return checkoutFromGroup(keyInfo->groupId);
		} else {
			queue.insert(key, true, false);
			wakeupEventLoop();
			return CheckoutResult(true, ServerPtr());
		}
	}

	void reportRequestRejected(const HashedStaticString &key, size_t uploadSize,
		unsigned long long uploadTime, const string &errorMessage)
	{
		boost::lock_guard<boost::mutex> l(syncher);
		KeyInfo *keyInfo;

		if (keys.lookup(key, &keyInfo)) {
			unsigned long long now = SystemTime::getUsec();
			keyInfo->lastRejectionErrorTime = now;
			server->reportRequestRejected(uploadSize, uploadTime, errorMessage, now);
			recreateBalancingList();
		}
	}

	void reportRequestDropped(const ServerPtr &server, size_t uploadSize,
		const string &errorMessage)
	{
		server->reportRequestDropped(uploadSize, errorMessage);
		boost::lock_guard<boost::mutex> l(syncher);
		recreateBalancingList();
	}
};


} // namespace RemoteSink
} // namespace UstRouter
} // namespace Passenger

#endif /* _PASSENGER_UST_ROUTER_REMOTE_SINK_SERVER_DATABASE_H_ */
