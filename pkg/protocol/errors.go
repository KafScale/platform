package protocol

const (
	NONE                        int16 = 0
	OFFSET_OUT_OF_RANGE         int16 = 1
	UNKNOWN_TOPIC_OR_PARTITION  int16 = 3
	UNKNOWN_SERVER_ERROR        int16 = -1
	REQUEST_TIMED_OUT           int16 = 7
	ILLEGAL_GENERATION          int16 = 22
	UNKNOWN_MEMBER_ID           int16 = 25
	REBALANCE_IN_PROGRESS       int16 = 27
	INVALID_TOPIC_EXCEPTION     int16 = 17
	TOPIC_ALREADY_EXISTS        int16 = 36
)
