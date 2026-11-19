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
src = SchemaRegistryClient({"url": "http://127.0.0.1:8085/apis/ccompat/v7"})
f = lambda name, fields: Schema('{"type":"record","name":"%s","namespace":"shop","fields":[%s]}' % (name, fields), "AVRO")
src.register_schema("orders-value", f("Order", '{"name":"id","type":"string"}'))
src.register_schema("orders-value", f("Order", '{"name":"id","type":"string"},{"name":"qty","type":"int","default":1}'))
src.register_schema("payments-value", f("Payment", '{"name":"order_id","type":"string"},{"name":"amount","type":"double"}'))
src.register_schema("customers-value", Schema('{"type":"object","properties":{"email":{"type":"string"}}}', "JSON"))
for s in src.get_subjects():
    print(s, [(v, src.get_version(s, v).schema_id) for v in src.get_versions(s)])
