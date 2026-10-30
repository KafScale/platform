// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package protocol

import "context"

type clientGoneKey struct{}

// ContextWithClientGone reports a closed client separately from request
// cancellation: already-read requests may still need to finish after EOF.
func ContextWithClientGone(ctx context.Context, gone <-chan struct{}) context.Context {
	return context.WithValue(ctx, clientGoneKey{}, gone)
}

// ClientGone returns the connection-close signal, or nil without a connection.
func ClientGone(ctx context.Context) <-chan struct{} {
	gone, _ := ctx.Value(clientGoneKey{}).(<-chan struct{})
	return gone
}

// ContextUntilClientGone is for waits that have no work to preserve after EOF.
// Produce must retain the original handler context to finish already-read data.
func ContextUntilClientGone(ctx context.Context) (context.Context, context.CancelFunc) {
	gone := ClientGone(ctx)
	if gone == nil {
		return ctx, func() {}
	}
	waiting, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-gone:
			cancel()
		case <-waiting.Done():
		}
	}()
	return waiting, func() { cancel(); <-done }
}
