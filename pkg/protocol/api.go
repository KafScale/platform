package protocol

// API keys supported by Kafscale in milestone 1.
const (
	APIKeyMetadata   int16 = 3
	APIKeyApiVersion int16 = 18
	APIKeyProduce    int16 = 0
)

// ApiVersion describes the supported version range for an API.
type ApiVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}
