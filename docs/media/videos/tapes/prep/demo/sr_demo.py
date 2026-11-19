# Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
sr = SchemaRegistryClient({"url": "http://127.0.0.1:8081"})
f = lambda fields: Schema('{"type":"record","name":"Order","namespace":"shop","fields":[%s]}' % fields, "AVRO")
base = '{"name":"id","type":"string"},{"name":"qty","type":"int","default":1}'
v3 = f(base + ',{"name":"note","type":"string","default":""}')
bad = f('{"name":"id","type":"int"}')
print("orders-value v3 compatible? ", sr.test_compatibility("orders-value", v3))
print("id:int compatible?         ", sr.test_compatibility("orders-value", bad))
print("register v3 -> id", sr.register_schema("orders-value", v3))
latest = sr.get_latest_version("orders-value")
print("latest:", latest.subject, "version", latest.version, "id", latest.schema_id)
