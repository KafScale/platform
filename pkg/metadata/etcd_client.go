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

package metadata

import (
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	etcdDialKeepAliveTime    = 10 * time.Second
	etcdDialKeepAliveTimeout = 3 * time.Second
	etcdReconnectMaxDelay    = time.Second
	etcdMinConnectTimeout    = time.Second
)

var etcdConnectParams = grpc.ConnectParams{
	Backoff: backoff.Config{
		BaseDelay:  100 * time.Millisecond,
		Multiplier: backoff.DefaultConfig.Multiplier,
		Jitter:     backoff.DefaultConfig.Jitter,
		MaxDelay:   etcdReconnectMaxDelay,
	},
	MinConnectTimeout: etcdMinConnectTimeout,
}

func EtcdClientConfig(cfg clientv3.Config) clientv3.Config {
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.DialKeepAliveTime == 0 {
		cfg.DialKeepAliveTime = etcdDialKeepAliveTime
	}
	if cfg.DialKeepAliveTimeout == 0 {
		cfg.DialKeepAliveTimeout = etcdDialKeepAliveTimeout
	}
	cfg.PermitWithoutStream = true
	cfg.DialOptions = append(append([]grpc.DialOption{}, cfg.DialOptions...), grpc.WithConnectParams(etcdConnectParams))
	return cfg
}
