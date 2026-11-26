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

package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestIsTemporaryAcceptError(t *testing.T) {
	acceptErr := func(err error) error {
		return &net.OpError{Op: "accept", Net: "tcp", Err: os.NewSyscallError("accept", err)}
	}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"too many open files", acceptErr(syscall.EMFILE), true},
		{"file table full", acceptErr(syscall.ENFILE), true},
		{"connection aborted", acceptErr(syscall.ECONNABORTED), true},
		{"interrupted", acceptErr(syscall.EINTR), true},
		{"timeout", &net.OpError{Op: "accept", Net: "tcp", Err: timeoutError{}}, true},
		{"listener closed", &net.OpError{Op: "accept", Net: "tcp", Err: net.ErrClosed}, false},
		{"wrapped listener closed", fmt.Errorf("serve: %w", net.ErrClosed), false},
		{"permission denied", acceptErr(syscall.EACCES), false},
		{"plain error", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTemporaryAcceptError(tc.err); got != tc.want {
				t.Fatalf("isTemporaryAcceptError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
