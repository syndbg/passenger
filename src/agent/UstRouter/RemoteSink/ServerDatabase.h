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
#include <UstRouter/RemoteSink/ServerGroup.h>

namespace Passenger {
namespace UstRouter {
namespace RemoteSink {

using namespace std;
using namespace boost;


class ServerDatabase {
public:
	struct KeyInfo {
		Group *group;
		unsigned long long lastCheckTime;
		unsigned long long lastRejectionErrorTime;
		unsigned long long recheckTimeoutWhenAllHealthy;
		unsigned long long recheckTimeoutWhenHaveErrors;

		KeyInfo()
			: group(NULL),
			  lastCheckTime(0),
			  lastRejectionErrorTime(0),
			  recheckTimeoutWhenAllHealthy(5 * 60 * 1000000),
			  recheckTimeoutWhenHaveErrors(60 * 1000000)
			{ }
	};

private:
	StringKeyTable<ServerGroup> groups;
	StringKeyTable<KeyInfo> keys;
	Group unknownGroup;

public:
	/*
	void setGroupedKeys(const vector< vector<string> > &groupedKeys, Migrator &migrator) {
		StringKeyTable<Group> newGroups;
		StringKeyTable<KeyMappingEntry> newKeyMapping;
		vector<string> groupNames;
		vector< vector<string> >::const_iterator g_it, g_end = groupedKeys.end();
		StringKeyTable<Group>::Cell *cell;
		unsigned int i;

		// Populate 'newGroups' with Group objects
		for (g_it = groupedKeys.begin(); g_it != g_end; g_it++) {
			string groupName = createGroupName(*g_it);
			groupNames.push_back(groupName);
			newGroups.insert(groupName, Group());
		}

		// Populate 'newKeyMapping'
		for (i = 0, g_it = groupedKeys.begin(); g_it != g_end; i++, g_it++) {
			const string &groupName = groupNames[i];
			const vector<string> &keys = *g_it;
			vector<string>::const_iterator k_it, k_end = keys.end();

			for (k_it = keys.begin(); k_it != k_end; k_it++) {
				StringKeyTable<Group>::Iterator newGroupIterator(
					newGroups.lookupCell(groupName));
				newKeyMapping.insert(key, KeyMappingEntry(newGroupIterator.getKey(),
					&newGroupIterator.getValue()));
			}
		}

		// Migrate transactions to the new groups using the new
		// key mapping
		StringKeyTable<Group>::Iterator it(groups);
		while (*it != NULL) {
			Group &group = it.getValue();
			migrator.migrateGroup(group, newKeyMapping);
			assert(group.empty());
			// for (transaction in group.queue) {
			// 	KeyMappingEntry entry = newKeyMapping[transaction->getUnionStationKey()];
			// 	move transaction to entry.group.queue;
			// }
			it.next();
		}

		// May not migrate over everything.
		migrator.migrateGroup(unknownGroup, newKeyMapping);

		groups = boost::move(newGroups);
		keyMapping = boost::move(newKeyMapping);
	}
	*/

	KeyInfo *getKeyInfo(const HashedStaticString &key) {
		KeyInfo *keyInfo;

		if (keys.lookup(key, &keyInfo)) {
			return keyInfo;
		} else {
			return &keys.insert(key, KeyInfo()).getValue();
		}
	}

	ServerGroup *groupKey(Transaction *transaction) {
		KeyInfo *keyInfo = getKeyInfo(transaction->getUnionStationKey());
		return keyInfo->group;
	}
};


} // namespace RemoteSink
} // namespace UstRouter
} // namespace Passenger

#endif /* _PASSENGER_UST_ROUTER_REMOTE_SINK_SERVER_DATABASE_H_ */
