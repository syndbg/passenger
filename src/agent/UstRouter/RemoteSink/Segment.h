class ServerGroup {
public:
	struct BatcherFields {
		TransactionList queue;
		size_t bytesAdded;
		size_t bytesQueued;
		size_t bytesProcessing;
		unsigned long long lastQueueAddTime;
		unsigned long long lastProcessingBeginTime;
		unsigned long long lastProcessingEndTime;
		unsigned int nQueued;
		unsigned int nProcessing;
	};

	struct SenderFields {
		typedef boost::container::small_vector<4, ServerPtr> SmallServerList;
		string serversHash;

		SmallServerList servers;
		SmallServerList balancingList;
		unsigned int nextServer;
		bool allHealthy;
	};

	BatcherFields batcherFields;
	char avoidFalseSharing __attribute__((aligned(64)));
	SenderFields senderFields;
};
