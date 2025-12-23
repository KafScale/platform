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

package mcpserver

import (
	"log"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	console "github.com/novatechflow/kafscale/internal/console"
	"github.com/novatechflow/kafscale/pkg/metadata"
)

type Options struct {
	Store   metadata.Store
	Metrics console.MetricsProvider
	Logger  *log.Logger
	Version string
}

func NewServer(opts Options) *mcp.Server {
	version := opts.Version
	if version == "" {
		version = "dev"
	}
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "kafscale-mcp",
		Version: version,
	}, nil)
	registerTools(server, opts)
	return server
}
