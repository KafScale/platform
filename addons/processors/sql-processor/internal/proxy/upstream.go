// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package proxy

import (
	"errors"
	"github.com/jackc/pgproto3/v2"
)

// pgproto3/v2 has no patched release for GO-2026-4518. Isolate its decoder:
// malformed upstream DataRow lengths can panic instead of returning an error.
// The caller stops relaying and closes the upstream and client connections.
func receiveUpstream(frontend *pgproto3.Frontend) (message pgproto3.BackendMessage, err error) {
	defer func() {
		if recover() != nil {
			message = nil
			err = errors.New("invalid upstream PostgreSQL message")
		}
	}()
	return frontend.Receive()
}
