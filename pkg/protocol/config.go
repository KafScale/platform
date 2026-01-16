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

// Config resource types.
const (
	ConfigResourceTopic  int8 = 2
	ConfigResourceBroker int8 = 4
)

// Config sources.
const (
	ConfigSourceUnknown       int8 = -1
	ConfigSourceDynamicTopic  int8 = 1
	ConfigSourceDynamicBroker int8 = 2
	ConfigSourceStaticBroker  int8 = 4
	ConfigSourceDefaultConfig int8 = 5
	ConfigSourceGroupConfig   int8 = 8
)

// Config types.
const (
	ConfigTypeBoolean  int8 = 1
	ConfigTypeString   int8 = 2
	ConfigTypeInt      int8 = 3
	ConfigTypeShort    int8 = 4
	ConfigTypeLong     int8 = 5
	ConfigTypeDouble   int8 = 6
	ConfigTypeList     int8 = 7
	ConfigTypeClass    int8 = 8
	ConfigTypePassword int8 = 9
)
