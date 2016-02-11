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
#ifndef _PASSENGER_UST_ROUTER_REMOTE_SINK_SERVER_DATABASE_ALGORITHM_H_
#define _PASSENGER_UST_ROUTER_REMOTE_SINK_SERVER_DATABASE_ALGORITHM_H_

#include <vector>
#include <algorithm>
#include <UstRouter/RemoteSink/ServerDefinition.h>

namespace Passenger {
namespace UstRouter {
namespace RemoteSink {

using namespace std;


class ServerDatabaseAlgorithm {
public:
	typedef vector<ServerDefinitionPtr> Vector;
	typedef Vector::iterator Iterator;

private:
	static Iterator findEqualServer(const Vector &servers,
		const ServerDefinitionPtr &server)
	{
		Iterator it, end = servers.end();
		for (it = servers.begin(); it != end; it++) {
			if (server->equals(*it.get())) {
				return it;
			}
		}
		return servers.end();
	}

public:
	static void updateServerList(Vector &oldServers,
		Vector &newServers, unsigned int &nextServerNumber)
	{
		Iterator oldIt, oldEnd = oldServers.end();
		vector<Iterator> oldServersToRemove;

		for (oldIt = oldServers.begin(); oldIt != oldEnd; oldIt++) {
			const ServerDefinitionPtr &oldServer = *oldIt;

			Iterator newIt = findEqualServer(newServers, oldServer);
			if (newIt == newServers.end()) {
				oldServersToRemove.push_back(oldIt);
			} else {
				oldServer->update(newServer);
				newServers.erase(newIt);
			}
		}

		while (!oldServersToRemove.empty()) {
			Iterator oldIt = oldServersToRemove.back();
			oldServers.erase(oldIt);
		}

		Iterator newIt, newEnd = newServers.end();
		for (newIt = newServers.begin(); newIt != newEnd; newIt++) {
			newIt->setNumber(nextServerNumber);
			nextServerNumber++;
			oldServers.push_back(*newIt);
		}

		newServers.clear();
	}

	static Vector indexServersByWeight(const Vector &servers) {
		Vector result;
		Iterator it, end = servers.end();

		for (it = servers.begin(); it != end; it++) {
			const ServerDefinitionPtr &server = *it;
			unsigned int weight = server->getWeight();
			for (unsigned int i = 0; i < weight; i++) {
				result.push_back(server);
			}
		}

		std::random_shuffle(result.begin(), result.end());

		return result;
	}
};


} // namespace RemoteSink
} // namespace UstRouter
} // namespace Passenger

#endif /* _PASSENGER_UST_ROUTER_REMOTE_SINK_SERVER_DATABASE_ALGORITHM_H_ */
