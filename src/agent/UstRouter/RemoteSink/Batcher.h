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
#ifndef _PASSENGER_UST_ROUTER_REMOTE_SINK_BATCHER_H_
#define _PASSENGER_UST_ROUTER_REMOTE_SINK_BATCHER_H_

#include <boost/container/small_vector.hpp>
#include <boost/bind.hpp>
#include <oxt/thread.hpp>
#include <iomanip>
#include <utility>
#include <cassert>
#include <cstddef>

#include <ev.h>

#include <Logging.h>
#include <DataStructures/StringKeyTable.h>
#include <Algorithms/MovingAverage.h>
#include <Utils/StrIntUtils.h>
#include <Utils/JsonUtils.h>
#include <Utils/SystemTime.h>
#include <Utils/VariantMap.h>
#include <UstRouter/Transaction.h>
#include <UstRouter/RemoteSink/Batch.h>
#include <UstRouter/RemoteSink/BatchingAlgorithm.h>

namespace Passenger {
namespace UstRouter {
namespace RemoteSink {

using namespace std;
using namespace boost;
using namespace oxt;


template<typename Sender>
class Batcher {
public:
	typedef boost::container::small_vector<Batch, 16> BatchList;

private:

	oxt::thread *thread;
	Sender *sender;

	mutable boost::mutex syncher;
	boost::condition_variable cond;
	StringKeyTable<Group> knownGroups;
	Group unknownGroup;
	size_t threshold, limit;
	size_t totalBytesAdded, totalBytesQueued, totalBytesProcessing;
	size_t bytesForwarded, bytesDropped, peakSize;
	double avgProcessingSpeed, avgCompressionFactor;
	unsigned int nForwarded, nDropped;
	int compressionLevel;
	bool quit: 1;
	bool quitImmediately: 1;

	void threadMain() {
		try {
			realThreadMain();
		} catch (const thread_interrupted &) {
			// Do nothing
		} catch (const tracable_exception &e) {
			P_WARN("ERROR: " << e.what() << "\n  Backtrace:\n" << e.backtrace());
		}
	}

	void realThreadMain() {
		TRACE_POINT();
		boost::unique_lock<boost::mutex> l(syncher);

		while (true) {
			UPDATE_TRACE_POINT();
			while (!quit && STAILQ_EMPTY(&queued)) {
				cond.wait(l);
			}

			if (quitImmediately) {
				return;
			}

			if (STAILQ_EMPTY(&queued)) {
				// If quitImmediately is not set, we honor the quit signal only after
				// having processed everything in the queue.
				if (quit) {
					return;
				}
			} else {
				UPDATE_TRACE_POINT();
				TransactionList transactions = consumeQueue();
				size_t bytesProcessing = this->bytesProcessing;
				size_t nProcessing = this->nProcessing;
				l.unlock();

				UPDATE_TRACE_POINT();
				pair<unsigned long long, size_t> batchResult = performBatching(
					transactions, bytesProcessing, nProcessing);

				UPDATE_TRACE_POINT();
				l.lock();
				updateProcessingStatistics(batchResult.first, batchResult.second);
			}
		}
	}

	TransactionList consumeQueue() {
		TRACE_POINT();
		TransactionList processing;

		P_ASSERT_EQ(bytesProcessing, 0);
		P_ASSERT_EQ(nProcessing, 0);

		bytesProcessing = bytesQueued;
		nProcessing = nQueued;
		STAILQ_SWAP(&queued, &processing, Transaction);
		bytesQueued = 0;
		nQueued = 0;
		lastProcessingBeginTime = SystemTime::getUsec();

		return processing;
	}

	pair<unsigned long long, size_t> performBatching(TransactionList &transactions,
		size_t bytesProcessing, size_t nProcessing)
	{
		TRACE_POINT();
		TransactionList undersizedTransactions, oversizedTransactions;

		P_DEBUG("[RemoteSink batcher] Compressing and creating batches for "
			<< nProcessing << " transactions ("
			<< (bytesProcessing / 1024) << " KB total)");

		STAILQ_INIT(&undersizedTransactions);
		STAILQ_INIT(&oversizedTransactions);

		BatchingAlgorithm::organizeTransactionsBySize(transactions,
			undersizedTransactions, oversizedTransactions, threshold);
		assert(STAILQ_EMPTY(&transactions));
		BatchingAlgorithm::organizeUndersizedTransactionsIntoBatches(
			undersizedTransactions, threshold);

		BatchList batches;
		BatchList::const_iterator it;
		unsigned long long startTime, endTime;
		size_t totalBatchSize = 0;

		UPDATE_TRACE_POINT();
		startTime = SystemTime::getUsec();
		BatchingAlgorithm::createBatchObjectsForUndersizedTransactions(
			undersizedTransactions, batches, compressionLevel);

		UPDATE_TRACE_POINT();
		BatchingAlgorithm::createBatchObjectsForOversizedTransactions(
			oversizedTransactions, batches, compressionLevel);
		endTime = SystemTime::getUsec();

		UPDATE_TRACE_POINT();
		for (it = batches.begin(); it != batches.end(); it++) {
			totalBatchSize += it->getDataSize();
		}

		P_DEBUG("[RemoteSink batcher] Compressed " << (bytesProcessing / 1024) << " KB to "
			<< (countTotalCompressedSize(batches) / 1024) << " KB in "
			<< std::fixed << std::setprecision(2) << ((endTime - startTime) / 1000000.0)
			<< " sec, created " << batches.size() << " batches totalling "
			<< (totalBatchSize / 1024) << " KB");
		sender->send(batches);

		return std::make_pair(endTime - startTime, totalBatchSize);
	}

	size_t countTotalCompressedSize(const BatchList &batches) const {
		BatchList::const_iterator it, end = batches.end();
		size_t result = 0;

		for (it = batches.begin(); it != end; it++) {
			result += it->getDataSize();
		}

		return result;
	}

	void updateProcessingStatistics(unsigned long long processingTime, size_t totalBatchSize) {
		avgProcessingSpeed = exponentialMovingAverage(avgProcessingSpeed,
			(bytesProcessing / 1024.0) / (processingTime / 1000000.0),
			0.5);
		avgCompressionFactor = exponentialMovingAverage(avgCompressionFactor,
			(double) totalBatchSize / bytesProcessing,
			0.5);
		bytesForwarded += bytesProcessing;
		nForwarded += nProcessing;
		bytesProcessing = 0;
		nProcessing = 0;
		lastProcessingEndTime = SystemTime::getUsec();
	}

	string getRecommendedBufferLimit() const {
		return toString(peakSize * 2 / 1024) + " KB";
	}

	Json::Value inspectTotalAsJson() const {
		Json::Value doc;

		doc["size"] = byteSizeToJson(bytesQueued + bytesProcessing);
		doc["count"] = nQueued + nProcessing;
		doc["peak_size"] = byteSizeToJson(peakSize);
		doc["limit"] = byteSizeToJson(limit);

		return doc;
	}

public:
	// TODO: different batch groups based on load balancer manifest responses
	Batcher(Sender *_sender, const VariantMap &options)
		: thread(NULL),
		  sender(_sender),
		  threshold(options.getULL("union_station_batch_threshold")),
		  limit(options.getULL("union_station_batch_limit")),
		  totalBytesAdded(0),
		  totalBytesQueued(0),
		  totalBytesProcessing(0),
		  bytesForwarded(0),
		  bytesDropped(0),
		  peakSize(0),
		  avgProcessingSpeed(AVG_NOT_AVAILABLE),
		  avgCompressionFactor(AVG_NOT_AVAILABLE),
		  nForwarded(0),
		  nDropped(0),
		  compressionLevel(options.getULL("union_station_compression_level",
		      false, Z_DEFAULT_COMPRESSION)),
		  quit(false)
		{ }

	~Batcher() {
		shutdown();
		waitForTermination();
	}

	void start() {
		thread = new oxt::thread(boost::bind(&Batcher::threadMain, this),
			"RemoteSink batcher", 1024 * 1024);
	}

	void shutdown(bool dropQueuedWork = false) {
		boost::lock_guard<boost::mutex> l(syncher);
		quit = true;
		quitImmediately = quitImmediately || dropQueuedWork;
		cond.notify_one();
	}

	void waitForTermination() {
		if (thread != NULL) {
			thread->join();
			delete thread;
			thread = NULL;
		}

		boost::lock_guard<boost::mutex> l(syncher);
		if (quitImmediately) {
			Transaction *transaction = STAILQ_FIRST(&queued);
			while (transaction != NULL) {
				Transaction *next = STAILQ_NEXT(transaction, next);
				delete transaction;
				transaction = next;
			}
			STAILQ_INIT(&queued);
		}
		assert(STAILQ_EMPTY(&queued));
	}

	void add(TransactionList &transactions, size_t totalBodySize, unsigned int count,
		size_t &bytesAdded, unsigned int &nAdded, ev_tstamp now)
	{
		boost::lock_guard<boost::mutex> l(syncher);

		bytesAdded = 0;
		nAdded = 0;
		peakSize = std::max(peakSize, totalBytesQueued + totalBytesProcessing + totalBodySize);

		while (nAdded < count && totalBytesQueued + totalBytesProcessing <= limit) {
			assert(!STAILQ_EMPTY(&transactions));
			Transaction *transaction = STAILQ_FIRST(&transactions);
			size_t size = transaction->getBody().size();
			STAILQ_REMOVE_HEAD(&transactions, next);

			Group *group = grouper.group(transaction);
			STAILQ_INSERT_TAIL(&group->queued, transaction, next);
			group->bytesAdded += size;
			group->bytesQueued += size;
			group->nAdded++;
			group->nQueued++;
			totalBytesAdded += size;
			totalBytesQueued += size;

			bytesAdded += size;
			nAdded++;
		}

		if (nAdded != count) {
			assert(totalBytesQueued + totalBytesProcessing > limit);
			assert(totalBodySize > bytesAdded);
			bytesDropped += totalBodySize - bytesAdded;
			nDropped += count - nAdded;

			if (compressionLevel > 3) {
				P_WARN("Unable to batch and compress Union Station data quickly enough. "
					"Please lower the compression level to speed up compression, or "
					"increase the batch buffer's limit (recommended limit: "
					+ getRecommendedBufferLimit() + ")");
			} else {
				P_WARN("Unable to batch and compress Union Station data quickly enough. "
					"The current compression level is " + toString(compressionLevel)
					+ ", which is already very fast. Please try increasing the batch "
					+ "buffer's limit (recommended limit: " + getRecommendedBufferLimit()
					+ ")");
			}
		}

		pass unknown keys to server database updater;

		cond.notify_one();
	}

	void onServerDatabaseUpdate(const vector< vector<string> > &groupedKeys) {

	}

	void setThreshold(size_t newThreshold) {
		boost::lock_guard<boost::mutex> l(syncher);
		threshold = newThreshold;
	}

	void setLimit(size_t newLimit) {
		boost::lock_guard<boost::mutex> l(syncher);
		limit = newLimit;
	}

	void setCompressionLevel(int newLevel) {
		boost::lock_guard<boost::mutex> l(syncher);
		compressionLevel = newLevel;
	}

	void configure(const Json::Value &config) {
		boost::lock_guard<boost::mutex> l(syncher);
		threshold = getJsonUint64Field(config, "union_station_batch_threshold",
			threshold);
		limit = getJsonUint64Field(config, "union_station_batch_limit",
			limit);
		compressionLevel = getJsonUint64Field(config, "union_station_compression_level",
			compressionLevel);
	}

	Json::Value inspectStateAsJson() const {
		Json::Value doc;
		boost::lock_guard<boost::mutex> l(syncher);

		doc["total_in_memory"] = inspectTotalAsJson();
		doc["threshold"] = byteSizeToJson(threshold);
		doc["compression_level"] = compressionLevel;
		doc["average_processing_speed"] = byteSpeedToJson(
			avgProcessingSpeed / 1000000.0, -1, "second");
		if (avgCompressionFactor == -1) {
			doc["average_compression_factor"] = Json::Value(Json::nullValue);
		} else {
			doc["average_compression_factor"] = avgCompressionFactor;
		}

		doc["queued"] = byteSizeAndCountToJson(bytesQueued, nQueued);
		doc["processing"] = byteSizeAndCountToJson(bytesProcessing, nProcessing);
		doc["forwarded"] = byteSizeAndCountToJson(bytesForwarded, nForwarded);
		doc["dropped"] = byteSizeAndCountToJson(bytesDropped, nDropped);

		if (lastQueueAddTime == 0) {
			doc["last_queue_add_time"] = Json::Value(Json::nullValue);
		} else {
			doc["last_queue_add_time"] = timeToJson(lastQueueAddTime);
		}
		if (lastProcessingBeginTime == 0) {
			doc["last_processing_begin_time"] = Json::Value(Json::nullValue);
		} else {
			doc["last_processing_begin_time"] = timeToJson(lastProcessingBeginTime);
		}
		if (lastProcessingEndTime == 0) {
			doc["last_processing_end_time"] = Json::Value(Json::nullValue);
		} else {
			doc["last_processing_end_time"] = timeToJson(lastProcessingEndTime);
		}
		if (lastProcessingBeginTime == 0 || lastProcessingEndTime == 0) {
			doc["last_processing_duration"] = Json::Value(Json::nullValue);
		} else {
			doc["last_processing_duration"] = durationToJson(
				lastProcessingEndTime - lastProcessingBeginTime);
		}

		return doc;
	}
};


} // namespace RemoteSink
} // namespace UstRouter
} // namespace Passenger

#endif /* _PASSENGER_UST_ROUTER_REMOTE_SINK_BATCHER_H_ */
