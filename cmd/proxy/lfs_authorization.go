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
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/KafScale/platform/pkg/acl"
)

// The LFS rewrite uploads to S3 before forwarding Produce. It must use the
// broker's ACL snapshot to avoid side effects for unauthorized topics.
func loadLFSAuthorizer(authenticated bool) (*acl.Authorizer, error) {
	if !authenticated || !envBoolDefault("KAFSCALE_PROXY_LFS_ENABLED", false) {
		return nil, nil
	}
	raw := []byte(strings.TrimSpace(os.Getenv("KAFSCALE_ACL_JSON")))
	if len(raw) == 0 {
		if path := strings.TrimSpace(os.Getenv("KAFSCALE_ACL_FILE")); path != "" {
			var err error
			raw, err = os.ReadFile(path)
			if err != nil {
				return nil, err
			}
		}
	}
	if len(raw) == 0 {
		return nil, errors.New("authenticated LFS requires the broker ACL configuration")
	}
	var config acl.Config
	if err := json.Unmarshal(raw, &config); err != nil {
		return nil, err
	}
	config.Enabled = true
	return acl.NewAuthorizer(config), nil
}
