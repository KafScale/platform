// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
)

var (
	errMetadataRequest  = errors.New("metadata request decoding failed")
	errMetadataBackend  = errors.New("metadata backend request failed")
	errMetadataResponse = errors.New("metadata response decoding failed")
)

// Log bounded phase codes, never arbitrary request-derived decoder errors.
func metadataFailureReason(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, errMetadataRequest):
		return "request_decode"
	case errors.Is(err, errMetadataBackend):
		return "backend_connection"
	case errors.Is(err, errMetadataResponse):
		return "response_decode"
	default:
		return "metadata_processing"
	}
}
