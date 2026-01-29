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

import "context"

// ConnContext carries connection-scoped identity data for auth decisions.
type ConnContext struct {
	Principal  string
	RemoteAddr string
	ProxyAddr  string
}

type connContextKey struct{}

// ContextWithConnInfo attaches connection info to a context.
func ContextWithConnInfo(ctx context.Context, info *ConnContext) context.Context {
	if info == nil {
		return ctx
	}
	return context.WithValue(ctx, connContextKey{}, info)
}

// ConnInfoFromContext returns connection info if present.
func ConnInfoFromContext(ctx context.Context) *ConnContext {
	if ctx == nil {
		return nil
	}
	if v := ctx.Value(connContextKey{}); v != nil {
		if info, ok := v.(*ConnContext); ok {
			return info
		}
	}
	return nil
}
