# Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

import sys

JSON_SCHEMA = '{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name","age"],"additionalProperties":false}'

from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    dual_schema_id_deserializer,
    header_schema_id_serializer,
)
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

SCHEMA = '{"type":"record","name":"User","namespace":"e2e","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}'


def order_class():
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory, timestamp_pb2

    pool = descriptor_pool.DescriptorPool()
    ts = descriptor_pb2.FileDescriptorProto()
    timestamp_pb2.DESCRIPTOR.CopyToProto(ts)
    pool.Add(ts)
    fd = descriptor_pb2.FileDescriptorProto(name="e2e/order.proto", package="e2e", syntax="proto3", dependency=["google/protobuf/timestamp.proto"])
    msg = fd.message_type.add(name="Order")
    msg.field.add(name="id", number=1, type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING, label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL)
    msg.field.add(name="qty", number=2, type=descriptor_pb2.FieldDescriptorProto.TYPE_INT32, label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL)
    msg.field.add(name="at", number=3, type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE, label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL, type_name=".google.protobuf.Timestamp")
    pool.Add(fd)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("e2e.Order"))


def protobuf(mode, client, ctx, path, n):
    from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer

    order = order_class()
    if mode == "proto-serialize":
        serializer = ProtobufSerializer(order, client, {"use.deprecated.format": False})
        with open(path, "w") as out:
            for i in range(n):
                record = order(id="python-%d" % i, qty=i)
                record.at.seconds = 1000 + i
                out.write(serializer(record, ctx).hex() + "\n")
        return
    deserializer = ProtobufDeserializer(order, {"use.deprecated.format": False}, client)
    with open(path) as src:
        for line in src:
            if line.strip():
                record = deserializer(bytes.fromhex(line.strip()), ctx)
                print("%s,%d,%d" % (record.id, record.qty, record.at.seconds))


def main():
    mode, url, topic, path = sys.argv[1:5]
    client = SchemaRegistryClient({"url": url})
    ctx = SerializationContext(topic, MessageField.VALUE)
    if mode.startswith("json-"):
        from confluent_kafka.schema_registry.json_schema import JSONSerializer, JSONDeserializer
        if mode == "json-serialize":
            serializer = JSONSerializer(JSON_SCHEMA, client)
            with open(path, "w") as out:
                for i in range(int(sys.argv[5])):
                    out.write(serializer({"name": "python-%d" % i, "age": i}, ctx).hex() + "\n")
        else:
            deserializer = JSONDeserializer(JSON_SCHEMA, schema_registry_client=client)
            with open(path) as src:
                for line in src:
                    if line.strip():
                        record = deserializer(bytes.fromhex(line.strip()), ctx)
                        print("%s,%d" % (record["name"], record["age"]))
        return
    if mode.startswith("proto-"):
        protobuf(mode, client, ctx, path, int(sys.argv[5]))
        return
    if mode == "guid-serialize":
        serializer = AvroSerializer(client, SCHEMA, conf={"schema.id.serializer": header_schema_id_serializer})
        with open(path, "w") as out:
            for i in range(int(sys.argv[5])):
                headers = []
                payload = serializer({"name": "python-%d" % i, "age": i}, SerializationContext(topic, MessageField.VALUE, headers))
                out.write("%s %s\n" % (dict(headers)["__value_schema_id"].hex(), payload.hex()))
        return
    if mode == "guid-deserialize":
        deserializer = AvroDeserializer(client, conf={"schema.id.deserializer": dual_schema_id_deserializer})
        with open(path) as src:
            for line in src:
                if line.strip():
                    header, payload = line.split()
                    headers = [("__value_schema_id", bytes.fromhex(header))]
                    record = deserializer(bytes.fromhex(payload), SerializationContext(topic, MessageField.VALUE, headers))
                    print("%s,%d" % (record["name"], record["age"]))
        return
    if mode == "serialize":
        serializer = AvroSerializer(client, SCHEMA)
        with open(path, "w") as out:
            for i in range(int(sys.argv[5])):
                out.write(serializer({"name": "python-%d" % i, "age": i}, ctx).hex() + "\n")
        return
    deserializer = AvroDeserializer(client)
    with open(path) as src:
        for line in src:
            if line.strip():
                record = deserializer(bytes.fromhex(line.strip()), ctx)
                print("%s,%d" % (record["name"], record["age"]))


if __name__ == "__main__":
    main()
