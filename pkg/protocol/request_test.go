package protocol

import "testing"

func TestParseApiVersionsRequest(t *testing.T) {
	w := newByteWriter(16)
	w.Int16(APIKeyApiVersion)
	w.Int16(0)
	w.Int32(42)
	w.NullableString(nil)

	header, req, err := ParseRequest(w.Bytes())
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyApiVersion || header.CorrelationID != 42 {
		t.Fatalf("unexpected header: %#v", header)
	}
	if _, ok := req.(*ApiVersionsRequest); !ok {
		t.Fatalf("expected ApiVersionsRequest got %T", req)
	}
}

func TestParseMetadataRequest(t *testing.T) {
	w := newByteWriter(64)
	w.Int16(APIKeyMetadata)
	w.Int16(0)
	w.Int32(7)
	clientID := "client-1"
	w.NullableString(&clientID)
	w.Int32(2)
	w.String("orders")
	w.String("payments")

	header, req, err := ParseRequest(w.Bytes())
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	metaReq, ok := req.(*MetadataRequest)
	if !ok {
		t.Fatalf("expected MetadataRequest got %T", req)
	}
	if len(metaReq.Topics) != 2 || metaReq.Topics[0] != "orders" {
		t.Fatalf("unexpected topics: %#v", metaReq.Topics)
	}
	if header.ClientID == nil || *header.ClientID != "client-1" {
		t.Fatalf("client id mismatch: %#v", header.ClientID)
	}
}

func TestParseProduceRequest(t *testing.T) {
	w := newByteWriter(128)
	w.Int16(APIKeyProduce)
	w.Int16(9)
	w.Int32(100)
	clientID := "producer-1"
	w.NullableString(&clientID)
	w.Int16(1) // acks
	w.Int32(1500)
	w.Int32(1)     // topic count
	w.String("orders")
	w.Int32(1)                 // partitions
	w.Int32(0)                // partition id
	batch := []byte("record") // placeholder bytes
	w.BytesWithLength(batch)

	header, req, err := ParseRequest(w.Bytes())
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyProduce {
		t.Fatalf("unexpected api key %d", header.APIKey)
	}
	produceReq, ok := req.(*ProduceRequest)
	if !ok {
		t.Fatalf("expected ProduceRequest got %T", req)
	}
	if produceReq.Acks != 1 || len(produceReq.Topics) != 1 {
		t.Fatalf("produce data mismatch: %#v", produceReq)
	}
	if string(produceReq.Topics[0].Partitions[0].Records) != "record" {
		t.Fatalf("records mismatch")
	}
}
