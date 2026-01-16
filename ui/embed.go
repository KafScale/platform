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

package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed public/*
var content embed.FS

// StaticHandler returns an http.Handler that serves the embedded UI assets.
func StaticHandler() (http.Handler, error) {
	sub, err := fs.Sub(content, "public")
	if err != nil {
		return nil, err
	}
	return http.FileServer(http.FS(sub)), nil
}
