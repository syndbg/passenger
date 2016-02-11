#include "TestSupport.h"
#include <Logging.h>
#include <UstRouter/RemoteSink/Batcher.h>

using namespace Passenger;
using namespace Passenger::UstRouter;
using namespace Passenger::UstRouter::RemoteSink;
using namespace std;

namespace tut {
	struct UstRouter_RemoteSink_BatcherTest {
		struct TestSender {
			mutable boost::mutex syncher;
			Batcher<TestSender>::BatchList sent;

			void send(Batcher<TestSender>::BatchList &transactions) {
				boost::lock_guard<boost::mutex> l(syncher);
				Batcher<TestSender>::BatchList::iterator it;

				for (it = transactions.begin(); it != transactions.end(); it++) {
					sent.push_back(boost::move(*it));
				}
			}

			unsigned int getCount() const {
				boost::lock_guard<boost::mutex> l(syncher);
				return sent.size();
			}
		};

		Batcher<TestSender> *batcher;
		TestSender sender;
		size_t bytesAdded;
		unsigned int nAdded;

		StaticString smallBody, mediumBody, largeBody;

		#define SMALL_TXN_SIZE 4u
		Transaction *smallTxn, *smallTxn2, *smallTxn3;
		#define MEDIUM_TXN_SIZE 6u
		Transaction *mediumTxn, *mediumTxn2;

		UstRouter_RemoteSink_BatcherTest()
			: sender()
		{
			batcher = NULL;
			bytesAdded = 0;
			nAdded = 0;

			smallBody = P_STATIC_STRING("234");
			mediumBody = P_STATIC_STRING("23456");
			largeBody = P_STATIC_STRING("2345678");

			smallTxn = new Transaction("txnId1", "groupName1", "nodeName1", "category1",
				"unionStationKey1", 1, "filters1");
			smallTxn->append(smallBody);

			smallTxn2 = new Transaction("txnId2", "groupName2", "nodeName2", "category2",
				"unionStationKey2", 2, "filters2");
			smallTxn2->append(smallBody);

			smallTxn3 = new Transaction("txnId3", "groupName3", "nodeName3", "category3",
				"unionStationKey3", 3, "filters3");
			smallTxn3->append(smallBody);

			mediumTxn = new Transaction("txnId1", "groupName1", "nodeName1", "category1",
				"unionStationKey1", 1, "filters1");
			mediumTxn->append(mediumBody);

			mediumTxn2 = new Transaction("txnId2", "groupName2", "nodeName2", "category2",
				"unionStationKey2", 2, "filters2");
			mediumTxn2->append(mediumBody);
		}

		~UstRouter_RemoteSink_BatcherTest() {
			delete batcher;
			delete smallTxn;
			delete smallTxn2;
			delete smallTxn3;
			delete mediumTxn;
			delete mediumTxn2;
			setLogLevel(DEFAULT_LOG_LEVEL);
		}
	};

	DEFINE_TEST_GROUP(UstRouter_RemoteSink_BatcherTest);

	TEST_METHOD(1) {
		set_test_name("add() compresses and batches transactions in the background and feeds them to the sender");

		VariantMap options;
		options.setInt("union_station_batch_threshold", 2 * SMALL_TXN_SIZE);
		options.setInt("union_station_batch_limit", 512);
		batcher = new Batcher<TestSender>(&sender, options);
		batcher->start();

		TransactionList transactions;
		STAILQ_INIT(&transactions);
		STAILQ_INSERT_TAIL(&transactions, smallTxn, next);
		STAILQ_INSERT_TAIL(&transactions, smallTxn2, next);
		STAILQ_INSERT_TAIL(&transactions, smallTxn3, next);

		batcher->add(transactions, 3 * SMALL_TXN_SIZE, 3,
			bytesAdded, nAdded, 0);
		ensure_equals(bytesAdded, 3 * SMALL_TXN_SIZE);
		ensure_equals(nAdded, 3u);
		ensure(STAILQ_EMPTY(&transactions));

		smallTxn = NULL;
		smallTxn2 = NULL;
		smallTxn3 = NULL;

		// smallTxn and smallTxn2 should be batched together,
		// while smallTxn3 is sent separately.
		EVENTUALLY(5,
			result = sender.getCount() == 2;
		);
	}

	TEST_METHOD(2) {
		set_test_name("add() refuses work if its limit has already been exceeded");

		VariantMap options;
		options.setInt("union_station_batch_threshold", 2 * SMALL_TXN_SIZE);
		options.setInt("union_station_batch_limit", 3 * SMALL_TXN_SIZE + 1);
		batcher = new Batcher<TestSender>(&sender, options);
		batcher->start();

		TransactionList transactions;
		STAILQ_INIT(&transactions);
		STAILQ_INSERT_TAIL(&transactions, smallTxn, next);

		batcher->add(transactions, SMALL_TXN_SIZE, 1,
			bytesAdded, nAdded, 0);
		smallTxn = NULL;

		ensure_equals("(1)", bytesAdded, SMALL_TXN_SIZE);
		ensure_equals("(2)", nAdded, 1u);
		ensure("(3)", STAILQ_EMPTY(&transactions));
		EVENTUALLY(5,
			result = sender.getCount() == 1;
		);

		STAILQ_INIT(&transactions);
		STAILQ_INSERT_TAIL(&transactions, smallTxn2, next);
		STAILQ_INSERT_TAIL(&transactions, smallTxn3, next);
		STAILQ_INSERT_TAIL(&transactions, mediumTxn, next);
		STAILQ_INSERT_TAIL(&transactions, mediumTxn2, next);

		setLogLevel(LVL_ERROR);
		batcher->add(transactions, 2 * SMALL_TXN_SIZE + 2 * MEDIUM_TXN_SIZE, 4,
			bytesAdded, nAdded, 0);
		smallTxn2 = NULL;
		smallTxn3 = NULL;
		mediumTxn = NULL;

		ensure_equals("(5)", bytesAdded, 2 * SMALL_TXN_SIZE + MEDIUM_TXN_SIZE);
		ensure_equals("(6)", nAdded, 3u);
		ensure_equals("(7)", STAILQ_FIRST(&transactions), mediumTxn2);
		ensure_equals("(8)", STAILQ_LAST(&transactions, Transaction, next), mediumTxn2);
		EVENTUALLY(5,
			result = sender.getCount() == 3;
		);
	}

	TEST_METHOD(3) {
		set_test_name("It updates statistics");

		VariantMap options;
		options.setInt("union_station_batch_threshold", 2 * SMALL_TXN_SIZE);
		options.setInt("union_station_batch_limit", 512);
		batcher = new Batcher<TestSender>(&sender, options);
		batcher->start();

		TransactionList transactions;
		STAILQ_INIT(&transactions);
		STAILQ_INSERT_TAIL(&transactions, smallTxn, next);
		STAILQ_INSERT_TAIL(&transactions, smallTxn2, next);
		STAILQ_INSERT_TAIL(&transactions, smallTxn3, next);

		batcher->add(transactions, 3 * SMALL_TXN_SIZE, 3,
			bytesAdded, nAdded, 0);

		smallTxn = NULL;
		smallTxn2 = NULL;
		smallTxn3 = NULL;

		// smallTxn and smallTxn2 should be batched together,
		// while smallTxn3 is sent separately.
		EVENTUALLY(5,
			result = batcher->inspectStateAsJson()["forwarded_count"].asUInt() == 3;
		);
		Json::Value doc = batcher->inspectStateAsJson();
		ensure(doc.isMember("average_processing_speed"));
	}
}
