package schema

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestJSONSchemaValidation(t *testing.T) {
	validator := &jsonRegistry{
		mode:    ModeLenient,
		baseURL: "http://schemas.example.com",
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path != "/orders.json" {
					return &http.Response{
						StatusCode: http.StatusNotFound,
						Body:       io.NopCloser(bytes.NewReader(nil)),
					}, nil
				}
				body := []byte(`{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}`)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(body)),
					Header:     http.Header{"Content-Type": []string{"application/json"}},
				}, nil
			}),
		},
		cacheTTL: time.Minute,
		compiled: map[string]cachedSchema{},
	}

	if err := validator.Validate(context.Background(), "orders", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("expected valid payload: %v", err)
	}

	if err := validator.Validate(context.Background(), "orders", []byte(`{"id":"bad"}`)); err == nil {
		t.Fatalf("expected invalid payload")
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
