/*
 *  Phusion Passenger - https://www.phusionpassenger.com/
 *  Copyright (c) 2010-2016 Phusion Holding B.V.
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
#ifndef _PASSENGER_UST_ROUTER_REMOTE_SINK_SERVER_H_
#define _PASSENGER_UST_ROUTER_REMOTE_SINK_SERVER_H_

#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <string>
#include <cassert>
#include <cstddef>

#include <Logging.h>
#include <ExponentialMovingAverage.h>
#include <Utils/JsonUtils.h>
#include <Utils/SystemTime.h>

namespace Passenger {
namespace UstRouter {
namespace RemoteSink {

using namespace std;


class Server {
private:
	const string pingURL, sinkURL;
	unsigned int number;

	mutable boost::mutex syncher;
	unsigned int weight;
	unsigned int nSent, nAccepted, nRejected, nDropped;
	size_t bytesSent, bytesAccepted, bytesRejected, bytesDropped;
	unsigned long long lastRequestBeginTime, lastRequestEndTime;
	unsigned long long lastAcceptTime, lastRejectionErrorTime, lastDropErrorTime;
	double avgUploadTime, avgUploadSpeed, avgServerProcessingTime;
	DiscExponentialAverage<700, 5 * 1000000, 10 * 1000000> bandwidthUsage;
	unsigned long long lastRejectionErrorTime, lastDropErrorTime;
	string lastRejectionErrorMessage, lastDropErrorMessage;
	bool up;

	Json::Value inspectBandwidthUsageAsJson() const {
		if (bandwidthUsage.available()) {
			Json::Value doc;
			doc["average"] = byteSpeedToJson(bandwidthUsage.average()
				* 60 * 1000000, "minute");
			doc["stddev"] = byteSpeedToJson(bandwidthUsage.stddev()
				* 60 * 1000000, "minute");
			return doc;
		} else {
			return Json::Value(Json::nullValue);
		}
	}

public:
	Server(const StaticString &baseURL, unsigned int _weight)
		: pingURL(baseURL + "/ping"),
		  sinkURL(baseURL + "/sink"),
		  number(0),
		  weight(_weight),
		  nSent(0),
		  nAccepted(0),
		  nRejected(0),
		  nDropped(0),
		  bytesSent(0),
		  bytesAccepted(0),
		  bytesRejected(0),
		  bytesDropped(0),
		  lastRequestBeginTime(0),
		  lastRequestEndTime(0),
		  lastAcceptTime(0),
		  lastRejectionErrorTime(0),
		  lastDropErrorTime(0),
		  avgUploadTime(-1),
		  avgUploadSpeed(-1),
		  avgServerProcessingTime(-1),
		  lastRejectionErrorTime(0),
		  lastDropErrorTime(0),
		  up(true)
	{
		assert(_weight > 0);
	}

	const string &getPingURL() const {
		return pingURL;
	}

	const string &getSinkURL() const {
		return sinkURL;
	}

	unsigned int getWeight() const {
		boost::lock_guard<boost::mutex> l(syncher);
		return weight;
	}

	bool isUp() const {
		boost::lock_guard<boost::mutex> l(syncher);
		return up;
	}

	void setUp(bool _up) {
		boost::lock_guard<boost::mutex> l(syncher);
		up = _up;
	}

	void setNumber(unsigned int n) {
		number = n;
	}

	bool equals(const Server &other) const {
		return pingURL == other.getPingURL()
			&& sinkURL == other.getSinkURL()
			&& weight == other.getWeight();
	}

	void update(const Server &other) {
		P_ASSERT_EQ(pingURL, other.getPingURL());
		P_ASSERT_EQ(sinkURL, other.getSinkURL());
		boost::lock_guard<boost::mutex> l(syncher);
		weight = other.getWeight();
		up = other.getUp();
	}

	void reportRequestBegin() {
		boost::lock_guard<boost::mutex> l(syncher);
		nSent++;
		nActiveRequests++;
		lastRequestBeginTime = SystemTime::getUsec();
	}

	void reportRequestAccepted(size_t uploadSize, unsigned long long uploadTime,
		unsigned long long serverProcessingTime)
	{
		boost::lock_guard<boost::mutex> l(syncher);
		unsigned long long now = SystemTime::getUsec();

		nAccepted++;
		nActiveRequests--;
		bytesAccepted += uploadSize;
		lastRequestEndTime = now;
		lastAcceptTime = now;

		avgUploadTime = exponentialMovingAverage(avgUploadTime, uploadTime, 0.5);
		avgUploadSpeed = exponentialMovingAverage(avgUploadSpeed,
			uploadSize / uploadTime, 0.5);
		avgServerProcessingTime = exponentialMovingAverage(
			avgServerProcessingTime, serverProcessingTime, 0.5);
		bandwidthUsage.update(uploadSize / uploadTime, now);
	}

	void reportRequestRejected(size_t uploadSize, unsigned long long uploadTime,
		const string &errorMessage, unsigned long long now = 0)
	{
		if (now == 0) {
			now = SystemTime::getUsec();
		}

		boost::lock_guard<boost::mutex> l(syncher);
		nRejected++;
		nActiveRequests--;
		bytesRejected += uploadSize;
		lastRequestEndTime = lastRejectionErrorTime = now;
		lastRejectionErrorMessage = errorMessage;

		avgUploadTime = exponentialMovingAverage(avgUploadTime, uploadTime, 0.5);
		avgUploadSpeed = exponentialMovingAverage(avgUploadSpeed,
			uploadSize / uploadTime, 0.5);
		bandwidthUsage.update(uploadSize / uploadTime, now);
	}

	void reportRequestDropped(size_t uploadSize, const string &errorMessage) {
		boost::lock_guard<boost::mutex> l(syncher);
		up = false;
		nDropped++;
		nActiveRequests--;
		bytesDropped += uploadSize;
		lastRequestEndTime = lastDropErrorTime = SystemTime::getUsec();
		lastDropErrorMessage = errorMessage;
	}

	Json::Value inspectStateAsJson() const {
		Json::Value doc;
		boost::lock_guard<boost::mutex> l(syncher);

		doc["number"] = number;
		doc["ping_url"] = pingURL;
		doc["sink_url"] = sinkURL;
		doc["weight"] = weight;
		doc["sent"] = byteSizeAndCountToJson(bytesSent, nSent);
		doc["accepted"] = byteSizeAndCountToJson(bytesAccepted, nAccepted);
		doc["rejected"] = byteSizeAndCountToJson(bytesRejected, nRejected);
		doc["dropped"] = byteSizeAndCountToJson(bytesDropped, nDropped);
		doc["average_upload_time"] = durationToJson(avgUploadTime);
		doc["average_upload_speed"] = byteSpeedToJson(
			avgUploadSpeed * 1000000.0, "second");
		doc["average_server_processing_time"] = durationToJson(
			avgServerProcessingTime);
		doc["bandwidth_usage"] = inspectBandwidthUsageAsJson();
		doc["up"] = up;

		if (!lastRejectionErrorMessage.empty()) {
			doc["last_rejection_error"] = timeToJson(lastRejectionErrorTime);
			doc["last_rejection_error"]["message"] = lastRejectionErrorMessage;
		}
		if (!lastDropErrorMessage.empty()) {
			doc["last_drop_error"] = timeToJson(lastDropErrorTime);
			doc["last_drop_error"]["message"] = lastDropErrorMessage;
		}

		return doc;
	}
};

typedef boost::shared_ptr<Server> ServerPtr;


} // namespace RemoteSink
} // namespace UstRouter
} // namespace Passenger

#endif /* _PASSENGER_UST_ROUTER_REMOTE_SINK_SERVER_H_ */
