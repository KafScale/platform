// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package lfs

import (
	"errors"
	"fmt"
)

// Sentinel errors for LFS operations.
var (
	ErrNoConsumer      = errors.New("no consumer configured for LFS resolution")
	ErrNoStreamFetcher = errors.New("no stream fetcher configured for streaming access")
)

// LfsError wraps lower-level LFS errors with context.
type LfsError struct {
	Op  string
	Err error
}

func (e *LfsError) Error() string {
	if e == nil {
		return "lfs error"
	}
	if e.Op == "" {
		return fmt.Sprintf("lfs error: %v", e.Err)
	}
	return fmt.Sprintf("lfs %s: %v", e.Op, e.Err)
}

func (e *LfsError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ChecksumError indicates a SHA256 mismatch.
type ChecksumError struct {
	Expected string
	Actual   string
}

func (e *ChecksumError) Error() string {
	return fmt.Sprintf("checksum mismatch: expected %s got %s", e.Expected, e.Actual)
}
