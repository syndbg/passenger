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
#ifndef _PASSENGER_UST_ROUTER_REMOTE_SINK_SENDER_H_
#define _PASSENGER_UST_ROUTER_REMOTE_SINK_SENDER_H_

#include <boost/move/move.hpp>
#include <boost/noncopyable.hpp>
#include <oxt/thread.hpp>
#include <oxt/system_calls.hpp>
#include <oxt/backtrace.hpp>
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <cassert>
#include <cerrno>

#include <sys/types.h>

#include <curl/curl.h>
#include <psg_sysqueue.h>

#include <Logging.h>
#include <Exceptions.h>
#include <Utils/IOUtils.h>
#include <Utils/StrIntUtils.h>
#include <Utils/SystemTime.h>
#include <Utils/ScopeGuard.h>
#include <UstRouter/RemoteSink/Batcher.h>
#include <UstRouter/RemoteSink/ServerList.h>

namespace Passenger {
namespace UstRouter {
namespace RemoteSink {

using namespace std;
using namespace boost;
using namespace oxt;


class Sender: public ServerList::Observer {
public:
	typedef Batcher<Sender>::BatchList BatchList;

private:
	enum TransferState {
		WAITING_FOR_SERVER_DEFINITION,
		NOT_YET_ACTIVE,
		ACTIVE_CONNECTING,
		ACTIVE_UPLOADING,
		ACTIVE_RECEIVING_RESPONSE
	};

	class Transfer: private boost::noncopyable {
	public:
		Sender *sender;
		TransferState state;
		unsigned int number;
		CURL *curl;
		Batch batch;
		ServerPtr server;
		unsigned long long lastActivity, startTime, uploadBeginTime, uploadEndTime;
		off_t alreadyUploaded;
		STAILQ_ENTRY(Transfer) next;
		string responseData;
		char errorBuf[CURL_ERROR_SIZE];

		Transfer(Sender *_sender)
			: sender(_sender),
			  state(WAITING_FOR_SERVER_DEFINITION),
			  number(0),
			  lastActivity(0),
			  startTime(0),
			  uploadBeginTime(0),
			  uploadEndTime(0),
			  alreadyUploaded(0)
		{
			curl = curl_easy_init();
			STAILQ_NEXT(this, next) = NULL;
		}

		~Transfer() {
			curl_easy_cleanup(curl);
		}
	};

	STAILQ_HEAD(TransferList, Transfer);

	CURLM *multi;
	mutable boost::mutex syncher;
	BatchList queued;
	TransferList transfersWaitingForServerDefinition;
	TransferList notYetActiveTransfers;
	TransferList activeTransfers;
	TransferList freeTransfers;
	size_t bytesQueued;
	size_t bytesWaitingForServerDefinition;
	size_t bytesInNotYetActiveTransfers;
	size_t bytesInActiveTransfers;
	size_t bytesDropped;
	size_t peakSize;
	unsigned int nWaitingForServerDefintion;
	unsigned int nNotYetActiveTransfers;
	unsigned int nActiveTransfers;
	unsigned int nFreeTransfers;
	unsigned int nDropped;
	unsigned int nPeakActiveTransfers;
	unsigned long long lastQueueAddTime, lastDropTime;
	unsigned int connectTimeout, uploadTimeout, responseTimeout;
	int wakeupPipe[2];
	bool quit: 1;
	bool quitImmediately: 1;

	void threadMain() {
		TRACE_POINT();
		try {
			bool done;

			multi = curl_multi_init();
			curl_multi_setopt(multi, CURLMOPT_PIPELINING, (long) CURLPIPE_MULTIPLEX);

			UPDATE_TRACE_POINT();
			mainLoop();

			curl_multi_cleanup(multi);

			UPDATE_TRACE_POINT();
			boost::lock_guard<boost::mutex> l(syncher);
			queued.clear();
			bytesQueued = 0;
			clearTransfers(&transfersWaitingForServerDefinition,
				&bytesWaitingForServerDefinition, &nWaitingForServerDefintion);
			clearTransfers(&notYetActiveTransfers, &bytesInNotYetActiveTransfers,
				&nNotYetActiveTransfers);
			clearTransfers(&activeTransfers, &bytesInActiveTransfers, &nActiveTransfers);
			clearTransfers(&freeTransfers, NULL, &nFreeTransfers);
		} catch (const thread_interrupted &) {
			// Do nothing
		} catch (const tracable_exception &e) {
			P_WARN("ERROR: " << e.what() << "\n  Backtrace:\n" << e.backtrace());
		}
	}

	void mainLoop() {
		TRACE_POINT();
		bool done = false;

		do {
			waitForEvents();
			try {
				done = processEvents();
			} catch (const thread_interrupted &) {
				// Do nothing
			} catch (const tracable_exception &e) {
				P_WARN("ERROR: " << e.what() << "\n  Backtrace:\n" << e.backtrace());
			}
		} while (!done);
	}

	void waitForEvents() {
		TRACE_POINT();
		CURLMcode ret;
		struct curl_waitfd extra_fds[1];
		long timeout;

		extra_fds[0].fd = wakeupPipe[0];
		extra_fds[0].events = CURL_WAIT_POLLIN;

		ret = curl_multi_timeout(multi, &timeout);
		if (ret != CURLM_OK) {
			P_ERROR("[RemoteSink sender] Error querying curl_multi_timeout(): " <<
				curl_multi_strerror(ret));
			timeout = 1000; // Arbitrary timeout to avoid busy-loop
		}

		ret = curl_multi_wait(multi, extra_fds,
			sizeof(extra_fds) / sizeof(struct curl_waitfd),
			timeout, NULL);
		if (ret != CURLM_OK) {
			P_ERROR("[RemoteSink sender] Error querying curl_multi_wait(): " <<
				curl_multi_strerror(ret));
			// Arbitrary sleep time to avoid busy-loop
			syscalls::usleep(1000000);
		}
	}

	bool processEvents() {
		TRACE_POINT();
		boost::lock_guard<boost::mutex> l(syncher);

		if (!queued.empty()) {
			processQueue();
		}
		if (!notYetActiveTransfers.empty()) {
			processNotYetActiveTransfers();
		}
		if (!activeTransfers.empty()) {
			processActiveTransfers();
		}
		readWakeupPipe();

		return quitImmediately || (quit && queued.empty()
			&& nWaitingForServerDefintion == 0 && nNotYetActiveTransfers == 0
			&& nActiveTransfers == 0);
	}

	void readWakeupPipe() {
		bool done = false;
		do {
			char buf[32];

			int ret = read(wakeupPipe[0], buf, sizeof(buf));
			if (ret == -1) {
				done = errno != EINTR;
				if (ret != AGAIN && ret != EWOULDBLOCK) {
					int e = errno;
					P_ERROR("[RemoteSink sender] Error reading from wakeup pipe: " <<
						strerror(e) << " (errno=" << e << ")");
				}
			} else if (ret == 0) {
				done = true;
			}
		} while (!done);
	}

	void processQueue() {
		TRACE_POINT();
		BatchList::iterator it, end = queued.end();

		for (it = queued.begin(); it != end; it++) {
			createTransfer(*it);
		}

		bytesQueued = 0;
		queued.clear();
	}

	void processNotYetActiveTransfers() {
		TRACE_POINT();
		Transfer *transfer, *next;

		transfer = STAILQ_FIRST(&notYetActiveTransfers);
		while (transfer != NULL) {
			next = STAILQ_NEXT(transfer, next);
			if (!startTransfer(transfer)) {
				finalizeActiveTransfer(transfer, (CURLcode) -1);
			}
			transfer = next;
		}

		bytesInNotYetActiveTransfers = 0;
		STAILQ_INIT(&notYetActiveTransfers);
	}

	void processActiveTransfers() {
		TRACE_POINT();
		CURLMcode code;
		CURLMsg *msg;

		do {
			code = curl_multi_perform(multi, NULL);
		} while (code == CURLM_CALL_MULTI_PERFORM);
		if (code != CURLM_OK) {
			P_ERROR("[RemoteSink sender] Error calling curl_multi_perform(): " <<
				curl_multi_strerror(code));
		}

		while ((msg = curl_multi_info_read(multi, &remaining)) != NULL) {
			if (msg->msg == CURLMSG_DONE) {
				Transfer *transfer = findActiveTransfer(msg->easy_handle);
				assert(transfer != NULL);
				finalizeActiveTransfer(transfer, msg->data.result);
			}
		}
	}

	void createTransfer(Batch &batch) {
		TRACE_POINT();
		if (STAILQ_EMPTY(&freeTransfers)) {
			transfer = new Transfer(this);
		} else {
			transfer = STAILQ_FIRST(&freeTransfers);
			STAILQ_REMOVE_HEAD(&freeTransfers, Transaction, next);
			nFreeTransfers--;
		}

		pair<bool, ServerPtr> p(serverDefinitionList->checkout(transfer));
		if (!p.first) {
			UPDATE_TRACE_POINT();
			batchDropped(batch);
			addTransferToFreelist(transfer);
			return;
		}

		UPDATE_TRACE_POINT();
		transfer->number = nextTransferNumber++;
		transfer->batch = boost::move(batch);
		transfer->server = boost::move(p.second);
		transfer->lastActivity = 0;
		transfer->startTime = 0;
		transfer->uploadEndTime = 0;
		transfer->alreadyUploaded = 0;

		if (p.second != NULL) {
			UPDATE_TRACE_POINT();
			transfer->state = NOT_YET_ACTIVE;
			if (!startTransfer(transfer)) {
				finalizeActiveTransfer(transfer, (CURLcode) -1);
			}
		} else {
			UPDATE_TRACE_POINT();
			transfer->state = WAITING_FOR_SERVER_DEFINITION;
			STAILQ_INSERT_TAIL(&transfersWaitingForServerDefinition, transfer, next);
			bytesWaitingForServerDefinition += transfer->batch.getDataSize();
			nWaitingForServerDefintion++;
		}
	}

	bool startTransfer(Transfer *transfer) {
		TRACE_POINT();
		CURL *curl = transfer->curl;
		const ServerPtr &server = transfer->server;

		P_ASSERT_EQ(transfer->state, NOT_YET_ACTIVE);
		assert(transfer->server != NULL);
		transfer->state = ACTIVE_CONNECTING;
		transfer->startTime = SystemTime::getUsec();
		STAILQ_INSERT_TAIL(&activeTransfers, transfer, next);
		bytesInActiveTransfers += transfer->batch.getDataSize();
		nActiveTransfers++;
		nPeakActiveTransfers = std::max(nActiveTransfers, nPeakActiveTransfers);

		UPDATE_TRACE_POINT();
		curl_easy_setopt(curl, CURLOPT_UPLOAD, (long) 1);
		curl_easy_setopt(curl, CURLOPT_URL, server->getSinkURL().c_str());
		curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long) CURL_HTTP_VERSION_2);
		curl_easy_setopt(curl, CURLOPT_PIPEWAIT, (long) 1);
		curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, (long) 1);
		curl_easy_setopt(curl, CURLOPT_NOPROGRESS, (long) 0);
		curl_easy_setopt(curl, CURLOPT_NOSIGNAL, (long) 0);
		curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, transfer->errorBuf);
		curl_easy_setopt(curl, CURLOPT_USERAGENT, PROGRAM_NAME " " PASSENGER_VERSION);
		curl_easy_setopt(curl, CURLOPT_POST, (long) 1);
		curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, (long) connectTimeout);
		curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, progressCallback);
		curl_easy_setopt(curl, CURLOPT_XFERINFODATA, transfer);
		curl_easy_setopt(curl, CURLOPT_READFUNCTION, readTransferData);
		curl_easy_setopt(curl, CURLOPT_READDATA, transfer);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE,
			(curl_off_t) transfer->batch.getDataSize());
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, handleResponseData);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, transfer);

		UPDATE_TRACE_POINT();
		CURLMcode code = curl_multi_add_handle(multi, curl);
		if (code != CURLM_OK) {
			P_ERROR("[RemoteSink sender] Error calling curl_multi_add_handle(): " <<
				curl_multi_strerror(code));
		}
		return code == CURLM_Ok;
	}

	static size_t readTransferData(char *buffer, size_t size, size_t nitems, void *instream) {
		TRACE_POINT();
		Transfer *transfer = (Transfer *) instream;
		StaticString data(transfer->batch.substr(transfer->alreadyUploaded, size * n));
		memcpy(buffer, data.data(), data.size());
		transfer->alreadyUploaded += data.size();
		return data.size();
	}

	static size_t handleResponseData(char *ptr, size_t size, size_t nmemb, void *userdata) {
		TRACE_POINT();
		Transfer *transfer = (Transfer *) userdata;
		transfer->responseData.append(ptr, size * nmemb);
		return size * nmemb;
	}

	static int progressCallback(void *clientp, curl_off_t dltotal, curl_off_t dlnow,
		curl_off_t ultotal, curl_off_t ulnow)
	{
		TRACE_POINT();
		Transfer *transfer = (Transfer *) clientp;
		Sender *self = transfer->sender;
		unsigned long long now = SystemTime::getUsec();

		transfer->lastActivity = now;

		switch (transfer->state) {
		case ACTIVE_CONNECTING:
			if (ultotal == ulnow) {
				// Upload done
				transfer->state = ACTIVE_RECEIVING_RESPONSE;
				transfer->uploadBeginTime = now;
				transfer->uploadEndTime = now;
			} else if (ulnow > 0) {
				// Upload in progress
				transfer->state = ACTIVE_UPLOADING;
				transfer->uploadBeginTime = now;
			}
			// libcurl automatically takes care of connection timeouts.
			return 0;
		case ACTIVE_UPLOADING:
			if (ultotal == ulnow) {
				// Upload done
				assert(transfer->uploadBeginTime > 0);
				transfer->state = ACTIVE_RECEIVING_RESPONSE;
				transfer->uploadEndTime = now;
				return 0;
			} else if (now >= transfer->startTime + self->uploadTimeout * 1000) {
				// Upload timeout
				return -1;
			} else {
				// Upload in progress
				return 0;
			}
		case ACTIVE_RECEIVING_RESPONSE:
			if (now >= transfer->startTime + self->responseTimeout * 1000) {
				// Timeout receiving response
				return -1;
			} else {
				// Response in progress
				return 0;
			}
		default:
			return 0;
		}
	}

	void addTransferToFreelist(Transfer *transfer) {
		TRACE_POINT();
		if (nFreeTransfers > MAX_FREE_TRANSFERS) {
			UPDATE_TRACE_POINT();
			delete transfer;
		} else {
			UPDATE_TRACE_POINT();
			transfer->server.reset();
			transfer->batch = Batch();
			transfer->responseData.clear();
			curl_easy_reset(transfer->curl);
			STAILQ_INSERT_HEAD(&freeTransfers, transfer, next);
			nFreeTransfers++;
		}
	}

	void batchDropped(const Batch &batch, bool quitting = false) {
		bytesDropped += batch.getDataSize();
		nDropped++;
		lastDropTime = SystemTime::getUsec();
	}

	Transfer *findActiveTransfer(CURL *curl) {
		Transfer *transfer;

		STAILQ_FOREACH(transfer, &activeTransfers, next) {
			if (transfer->curl == curl) {
				return transfer;
			}
		}

		return NULL;
	}

	void finalizeActiveTransfer(Transfer *transfer, CURLcode code) {
		TRACE_POINT();

		if (code == CURLE_OK) {
			handleResponse(transfer);
		} else {
			handleTransferPerformError(transfer);
		}

		code = curl_easy_getinfo(transfer->curl, CURLINFO_RESPONSE_CODE);

		unsigned long long now = SystemTime::getUsec();
		unsigned long long uploadTime = transfer->uploadEndTime - transfer->startTime;
		unsigned long long responseTime = now - transfer->uploadEndTime;
		if (error) {
			serverList->reportDown(transfer->server);
		} else {
			transfer->server->reportTimings(transfer->batch.getDataSize(),
				uploadTime, responseTime);
		}

		assert(transfer->state >= ACTIVE_CONNECTING);
		assert(bytesInActiveTransfers >= transfer->batch.getDataSize());
		assert(nActiveTransfers > 0);

		STAILQ_REMOVE(&activeTransfers, transfer, Transfer, next);
		bytesInActiveTransfers -= transfer->batch.getDataSize();
		nActiveTransfers--;

		curl_multi_remove_handle(multi, transfer->curl);
		addTransferToFreelist(transfer);
	}

	void handleResponse(Transfer *transfer) {
		Json::Reader reader;
		Json::Value response;
		long httpCode = -1;

		curl_easy_getinfo(transfer->curl, CURLINFO_RESPONSE_CODE, &httpCode);

		if (!reader.parse(transfer->responseData, response, false) || !validateResponse(response)) {
			recordTransferError(transfer,
				"The Union Station gateway server " + ip +
				" encountered an error while processing sent analytics data. "
				"It sent an invalid response. Key: " + item.unionStationKey
				+ ". Parse error: " + reader.getFormattedErrorMessages()
				+ "; HTTP code: " + toString(httpCode)
				+ "; data: \"" + cEscapeString(responseBody) + "\"");
			return SR_MALFUNCTION;
		} else if (response["status"].asString() == "ok") {
			if (httpCode == 200) {
				handleResponseSuccess(transfer);
				P_DEBUG("The Union Station gateway server " << ip
					<< " accepted the packet. Key: "
					<< item.unionStationKey);
				return SR_OK;
			} else {
				recordTransferError(transfer,
					"The Union Station gateway server " + ip
					+ " encountered an error while processing sent "
					"analytics data. It sent an invalid response. Key: "
					+ item.unionStationKey + ". HTTP code: "
					+ toString(httpCode) + ". Data: \""
					+ cEscapeString(responseBody) + "\"");
				return SR_MALFUNCTION;
			}
		} else {
			// response == error
			setPacketRejectedError(
				"The Union Station gateway server "
				+ ip + " did not accept the sent analytics data. "
				"Key: " + item.unionStationKey + ". "
				"Error: " + response["message"].asString());
			return SR_REJECTED;
		}
	}

	void handleTransferPerformError(Transfer *transfer) {

	}

	void wakeupEventLoop() {
		int ret;
		do {
			ret = write(wakeupPipe[1], "x", 1);
		} while (ret == -1 && errno == EINTR);
		if (ret == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
			int e = errno;
			P_ERROR("[RemoteSink sender] Error writing to wakeup pipe: " <<
				strerror(e) << " (errno=" << e << ")");
		}
	}

	void clearTransfers(TransferList *list, size_t *bytes, unsigned int *count) {
		Transfer *transfer = STAILQ_FIRST(list);
		while (transfer != NULL) {
			Transfer *next = STAILQ_NEXT(transfer, next);
			delete transfer;
			transfer = next;
		}
		STAILQ_INIT(list);

		if (bytes != NULL) {
			bytes = 0;
		}
		if (count != NULL) {
			count = 0;
		}
	}

	size_t totalSize() const {
		return bytesQueued + bytesWaitingForServerDefinition
			+ bytesInNotYetActiveTransfers + bytesInActiveTransfers;
	}

	size_t totalCount() const {
		return queued.size() + nWaitingForServerDefintion
			+ nNotYetActiveTransfers + nActiveTransfers;
	}

	Json::Value inspectTotalAsJson() const {
		Json::Value doc;

		doc["size"] = byteSizeToJson(totalSize());
		doc["count"] = totalCount();
		doc["peak_size"] = byteSizeToJson(peakSize);
		doc["limit"] = byteSizeToJson(limit);

		return doc;
	}

	Json::Value inspectQueuedAsJson() const {
		Json::Value doc = byteSizeAndCountToJson(bytesQueued, queued.size());
		doc["last_time_added_to"] = timeToJson(lastQueueAddTime);
		return doc;
	}

	Json::Value inspectAllTransfersAsJson() const {
		Json::Value doc, subdoc;

		subdoc = byteSizeAndCountToJson(bytesWaitingForServerDefinition,
			nWaitingForServerDefintion);
		inspectTransferListAsJson(&transfersWaitingForServerDefinition, subdoc);
		doc["waiting_for_server_definition"] = subdoc;

		subdoc = byteSizeAndCountToJson(bytesInNotYetActiveTransfers,
			nNotYetActiveTransfers);
		inspectTransferListAsJson(&notYetActiveTransfers, subdoc);
		doc["not_yet_active"] = subdoc;

		subdoc = byteSizeAndCountToJson(bytesInActiveTransfers, nActiveTransfers);
		subdoc["peak_count"] = nPeakActiveTransfers;
		inspectTransferListAsJson(&activeTransfers, subdoc);
		doc["active"] = subdoc;

		subdoc = Json::Value();
		subdoc["count"] = nFreeTransfers;
		doc["freelist"] = subdoc;

		return doc;
	}

	void inspectTransferListAsJson(Json::Value &doc, TransferList &transfers) const {
		Transaction *transaction;

		STAILQ_FOREACH(transfer, &transfers, next) {
			Json::Value subdoc;

			subdoc["state"] = getStateString(transfer->state).toString();
			subdoc["server"] = transfer->server->inspectStateAsJson();
			subdoc["data_size"] = transfer->batch.getDataSize();
			if (transfer->lastActivity == 0) {
				subdoc["last_activity"] = Json::Value(Json::nullValue);
			} else {
				subdoc["last_activity"] = timeToJson(transfer->lastActivity);
			}
			if (transfer->startTime == 0) {
				subdoc["start_time"] = Json::Value(Json::nullValue);
			} else {
				subdoc["start_time"] = timeToJson(transfer->startTime);
			}
			if (transfer->uploadBeginTime == 0) {
				subdoc["upload_begin_time"] = Json::Value(Json::nullValue);
			} else {
				subdoc["upload_begin_time"] = timeToJson(transfer->uploadBeginTime);
			}
			if (transfer->uploadEndTime == 0) {
				subdoc["upload_end_time"] = Json::Value(Json::nullValue);
			} else {
				subdoc["upload_end_time"] = timeToJson(transfer->uploadEndTime);
			}
			subdoc["already_uploaded"] = byteSizeToJson((size_t) transfer->alreadyUploaded);

			doc[toString(transfer->number)] = subdoc;
		}
	}

	StaticString getStateString(TransferState state) const {
		switch (state) {
		case WAITING_FOR_SERVER_DEFINITION,
			return P_STATIC_STRING("WAITING_FOR_SERVER_DEFINITION");
		case NOT_YET_ACTIVE,
			return P_STATIC_STRING("NOT_YET_ACTIVE");
		case ACTIVE_CONNECTING:
			return P_STATIC_STRING("ACTIVE_CONNECTING");
		case ACTIVE_UPLOADING:
			return P_STATIC_STRING("ACTIVE_UPLOADING");
		case ACTIVE_RECEIVING_RESPONSE:
			return P_STATIC_STRING("ACTIVE_RECEIVING_RESPONSE");
		default:
			return P_STATIC_STRING("UNKNOWN");
		}
	}

	Json::Value inspectDroppedAsJson() const {
		Json::Value doc;
		doc["count"] = nDropped;
		doc["size"] = byteSizeToJson(bytesDropped);
		doc["last_time"] = timeToJson(lastDropTime);
		return doc;
	}

public:
	// TODO: handle proxy
	// TODO: change recheck time based on response header
	// TODO: rename Transfer to Request
	Sender(const VariantMap &options)
		: thread(NULL),
		  multi(NULL),
		  bytesQueued(0),
		  bytesWaitingForServerDefinition(0),
		  bytesInNotYetActiveTransfers(0),
		  bytesInActiveTransfers(0),
		  bytesDropped(0),
		  peakSize(0),
		  nWaitingForServerDefintion(0),
		  nNotYetActiveTransfers(0),
		  nActiveTransfers(0),
		  nFreeTransfers(0),
		  nDropped(0),
		  nPeakActiveTransfers(0),
		  lastQueueAddTime(0),
		  lastDropTime(0),
		  connectTimeout(options.getUint("union_station_connect_timeout", false, 0)),
		  uploadTimeout(options.getUint("union_station_upload_timeout")),
		  responseTimeout(options.getUint("union_station_response_timeout")),
		  quit(false),
		  quitImmediately(false)
	{
		STAILQ_INIT(&transfersWaitingForServerDefinition);
		STAILQ_INIT(&notYetActiveTransfers);
		STAILQ_INIT(&activeTransfers);
		STAILQ_INIT(&freeTransfers);

		if (syscalls::pipe(wakeupPipe) != 0) {
			int e = errno;
			throw SystemException("Cannot create a pipe", e);
		}

		FdGuard g0(wakeupPipe[0]);
		FdGuard g1(wakeupPipe[1]);
		setNonBlocking(wakeupPipe[0]);
		setNonBlocking(wakeupPipe[1]);
		g0.clear();
		g1.clear();
	}

	~Sender() {
		TRACE_POINT();
		shutdown();
		waitForTermination();
	}

	void start() {
		thread = new oxt::thread(boost::bind(&Sender::threadMain, this),
			"RemoteSink sender", 1024 * 1024);
	}

	void shutdown(bool dropQueuedWork = false) {
		TRACE_POINT();
		boost::unique_lock<boost::mutex> l(syncher);
		quit = true;
		quitImmediately = quitImmediately || dropQueuedWork;
		l.unlock();
		wakeupEventLoop();
	}

	void waitForTermination() {
		TRACE_POINT();
		if (thread != NULL) {
			UPDATE_TRACE_POINT();
			thread->join();
			delete thread;
			thread = NULL;
		}

		UPDATE_TRACE_POINT();
		boost::unique_lock<boost::mutex> l(syncher);

		assert(queued.empty());
		assert(STAILQ_EMPTY(&transfersWaitingForServerDefinition));
		assert(STAILQ_EMPTY(&notYetActiveTransfers));
		assert(STAILQ_EMPTY(&activeTransfers));
		assert(STAILQ_EMPTY(&freeTransfers));

		P_ASSERT_EQ(bytesQueued, 0);
		P_ASSERT_EQ(bytesWaitingForServerDefinition, 0);
		P_ASSERT_EQ(bytesInNotYetActiveTransfers, 0);
		P_ASSERT_EQ(bytesInActiveTransfers, 0);

		P_ASSERT_EQ(nWaitingForServerDefintion, 0);
		P_ASSERT_EQ(nNotYetActiveTransfers, 0);
		P_ASSERT_EQ(nActiveTransfers, 0);
		P_ASSERT_EQ(nFreeTransfers, 0);
	}

	void send(BatchList &batches) {
		TRACE_POINT();
		boost::unique_lock<boost::mutex> l(syncher);

		BatchList::iterator it, end = batches.end();
		if (quit) {
			for (it = batches.begin(); it != end; it++) {
				batchDropped(*it, true);
			}
		} else {
			for (it = batches.begin(); it != end; it++) {
				if (totalSize() >= limit) {
					batchDropped(*it);
				} else {
					queued.push_back(boost::move(*it));
					bytesQueued += it->getDataSize();
				}
			}

			lastQueueAddTime = SystemTime::getUsec();
			peakSize = std::max(peakSize, totalSize());
		}
	}

	void serverDefinitionCheckedOut(const vector< pair<ServerPtr, void *> > &result) {
		TRACE_POINT();
		vector< pair<ServerPtr, void *> >::iterator it, end = result.end();
		bool wakeup = false;
		boost::unique_lock<boost::mutex> l(syncher);

		for (it = result.begin(); it != end; it++) {
			const ServerPtr &server = it->first;
			Transfer *transfer = static_cast<Transfer *>(it->second);

			P_ASSERT_EQ(transfer->state, WAITING_FOR_SERVER_DEFINITION);
			assert(transfer->server == NULL);
			assert(bytesWaitingForServerDefinition >= transfer->batch.getDataSize());
			assert(nWaitingForServerDefintion > 0);

			if (server != NULL) {
				STAILQ_REMOVE(&transfersWaitingForServerDefinition, transfer, Transfer, next);
				bytesWaitingForServerDefinition -= transfer->batch.getDataSize();
				nWaitingForServerDefintion--;

				transfer->state = NOT_YET_ACTIVE;
				transfer->server = server;
				STAILQ_INSERT_HEAD(&notYetActiveTransfers, transfer, next);
				bytesInNotYetActiveTransfers += transfer->batch.getDataSize();
				nNotYetActiveTransfers++;

				wakeup = true;
			} else {
				batchDropped(transfer->batch);
				curl_multi_remove_handle(multi, transfer->curl);
				addTransferToFreelist(transfer);
			}
		}

		if (wakekup) {
			l.unlock();
			wakeupEventLoop();
		}
	}

	Json::Value inspectStateAsJson() const {
		Json::Value doc, subdoc;
		boost::lock_guard<boost::mutex> l(syncher);

		doc["total"] = inspectTotalAsJson();
		doc["queued"] = inspectQueuedAsJson();
		doc["transfers"] = inspectAllTransfersAsJson();
		doc["dropped"] = inspectDroppedAsJson();

		return doc;
	}
};


} // namespace RemoteSink
} // namespace UstRouter
} // namespace Passenger

#endif /* _PASSENGER_UST_ROUTER_REMOTE_SINK_SENDER_H_ */
