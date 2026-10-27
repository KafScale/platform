// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").
package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestMetadataFailureReasonsDoNotLogRequestData(t *testing.T) {
	private := errors.New("request contains APIKey and private data")
	for _, test := range []struct {
		err    error
		reason string
	}{
		{context.DeadlineExceeded, "timeout"}, {context.Canceled, "canceled"},
		{errors.Join(errMetadataRequest, private), "request_decode"},
		{errors.Join(errMetadataBackend, private), "backend_connection"},
		{errors.Join(errMetadataResponse, private), "response_decode"},
		{private, "metadata_processing"},
	} {
		reason := metadataFailureReason(test.err)
		if reason != test.reason || strings.Contains(reason, "private") || strings.Contains(reason, "APIKey") {
			t.Fatal("unsafe metadata log reason", reason)
		}
	}
}
