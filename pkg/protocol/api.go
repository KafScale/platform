package protocol

// API keys supported by Kafscale in milestone 1.
const (
	APIKeyProduce         int16 = 0
	APIKeyFetch           int16 = 1
	APIKeyMetadata        int16 = 3
	APIKeyOffsetCommit    int16 = 8
	APIKeyOffsetFetch     int16 = 9
	APIKeyFindCoordinator int16 = 10
	APIKeyJoinGroup       int16 = 11
	APIKeyHeartbeat       int16 = 12
	APIKeyLeaveGroup      int16 = 13
	APIKeySyncGroup       int16 = 14
	APIKeyApiVersion      int16 = 18
	APIKeyCreateTopics    int16 = 19
	APIKeyDeleteTopics    int16 = 20
	APIKeyListOffsets     int16 = 2
)

// ApiVersion describes the supported version range for an API.
type ApiVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}
