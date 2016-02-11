class Segmenter {
private:
	KeyInfoPtr findOrCreateKeyInfo(const HashedStaticString &key) {
		KeyInfoPtr *keyInfo;

		if (keyInfos.lookup(key, &keyInfo)) {
			return *keyInfo;
		} else {
			KeyInfoPtr newKeyInfo(make_shared<KeyInfo>());
			keyInfos.insert(key, newKeyInfo);
			STAILQ_INSERT_TAIL(&unknownKeyInfos, newKeyInfo.get(), next);
			initiateApiLookup(newKeyInfo);
			return newKeyInfo;
		}
	}

protected:
	virtual void initiateApiLookup(const KeyInfoPtr &keyInfo) {
		keyInfo->lookingUp = true;
	}

public:
	void schedule(TransactionList &transactions, unsigned int count, unsigned int &nAdded) {
		SegmentList segmentsToForward;
		Segment *segment;

		STAILQ_INIT(&segmentsToForward);

		nAdded = 0;

		while (nAdded < count && bytesQueued + bytesProcessing <= limit) {
			Transaction *transaction = STAILQ_FIRST(&transactions);
			STAILQ_REMOVE_HEAD(&transactions, next);

			KeyInfoPtr keyInfo(findOrCreateKeyInfo(transaction->getUnionStationKey()));
			segment = keyInfo->segment.get();
			if (segment != NULL) {
				segment->bytesIncomingTransactions += transaction->getBody().size();
				segment->nIncomingTransactions++;
				segment->forwardToBatcher = true;
				STAILQ_INSERT_TAIL(&segment->incomingTransactions, transaction, next);
				STAILQ_INSERT_TAIL(&segmentsToForward, segment, nextToForwardToBatcher);
			} else {
				assert(keyInfo->lookingUp);
				bytesQueued += transaction->getBody().size();
				nQueued++;
				STAILQ_INSERT_TAIL(&queue, transaction, next);
			}

			nAdded++;
		}

		if (nAdded != count) {
			warn;
		}

		STAILQ_FOREACH(segment, &segmentsToForward, nextToForwardToBatcher) {
			segment->forwardToBatcher = false;
		}
		batcher.schedule(segmentsToForward);
	}

	void keyInfoApiLookupFinished(const string &key, CURLcode code, long httpCode, const string &body) {

	}
};
