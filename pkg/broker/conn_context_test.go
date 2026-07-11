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

package broker

import (
	"context"
	"testing"
)

func TestContextWithConnInfo(t *testing.T) {
	ctx := context.Background()
	info := &ConnContext{Principal: "user-1", RemoteAddr: "1.2.3.4:5678"}

	ctx2 := ContextWithConnInfo(ctx, info)
	got := ConnInfoFromContext(ctx2)
	if got == nil || got.Principal != "user-1" || got.RemoteAddr != "1.2.3.4:5678" {
		t.Fatalf("expected conn info, got %+v", got)
	}
}

func TestContextWithConnInfoNil(t *testing.T) {
	ctx := context.Background()
	ctx2 := ContextWithConnInfo(ctx, nil)
	got := ConnInfoFromContext(ctx2)
	if got != nil {
		t.Fatalf("expected nil for nil input, got %+v", got)
	}
}

func TestConnInfoFromContextNilContext(t *testing.T) {
	got := ConnInfoFromContext(nil)
	if got != nil {
		t.Fatalf("expected nil for nil context, got %+v", got)
	}
}

func TestConnInfoFromContextMissing(t *testing.T) {
	got := ConnInfoFromContext(context.Background())
	if got != nil {
		t.Fatalf("expected nil for empty context, got %+v", got)
	}
}

func TestConnContextProxyAddr(t *testing.T) {
	ctx := context.Background()
	info := &ConnContext{
		Principal:  "admin",
		RemoteAddr: "10.0.0.1:1234",
		ProxyAddr:  "10.0.0.100:443",
	}
	ctx2 := ContextWithConnInfo(ctx, info)
	got := ConnInfoFromContext(ctx2)
	if got.ProxyAddr != "10.0.0.100:443" {
		t.Fatalf("expected proxy addr, got %q", got.ProxyAddr)
	}
}
