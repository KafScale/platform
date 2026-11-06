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

package io.kafscale.e2e;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

public final class Main {
    private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"e2e\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        String url = args[1];
        String topic = args[2];
        Path file = Path.of(args[3]);
        Map<String, Object> config = Map.of("schema.registry.url", url, "auto.register.schemas", "serialize".equals(mode));
        if (mode.startsWith("json-")) {
            ObjectMapper mapper = new ObjectMapper();
            if ("json-serialize".equals(mode)) {
                JsonNode jsonSchema = mapper.readTree("{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"age\":{\"type\":\"integer\"}},\"required\":[\"name\",\"age\"],\"additionalProperties\":false}");
                List<String> lines = new ArrayList<>();
                try (KafkaJsonSchemaSerializer<JsonNode> serializer = new KafkaJsonSchemaSerializer<>()) {
                    serializer.configure(Map.of("schema.registry.url", url), false);
                    for (int i = 0; i < Integer.parseInt(args[4]); i++) {
                        JsonNode record = mapper.valueToTree(Map.of("name", "java-" + i, "age", i));
                        lines.add(HexFormat.of().formatHex(serializer.serialize(topic, JsonSchemaUtils.envelope(jsonSchema, record))));
                    }
                }
                Files.write(file, lines, StandardCharsets.UTF_8);
            } else {
                try (KafkaJsonSchemaDeserializer<JsonNode> deserializer = new KafkaJsonSchemaDeserializer<>()) {
                    deserializer.configure(Map.of("schema.registry.url", url, "json.value.type", JsonNode.class.getName()), false);
                    for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                        if (!line.isBlank()) {
                            JsonNode record = deserializer.deserialize(topic, HexFormat.of().parseHex(line.trim()));
                            System.out.println(record.get("name").asText() + "," + record.get("age").asInt());
                        }
                    }
                }
            }
            return;
        }
        if ("guid-serialize".equals(mode)) {
            Schema schema = new Schema.Parser().parse(SCHEMA);
            List<String> lines = new ArrayList<>();
            Map<String, Object> headerConfig = Map.of("schema.registry.url", url, "value.schema.id.serializer", "io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer");
            try (KafkaAvroSerializer serializer = new KafkaAvroSerializer()) {
                serializer.configure(headerConfig, false);
                for (int i = 0; i < Integer.parseInt(args[4]); i++) {
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("name", "java-" + i);
                    record.put("age", i);
                    Headers headers = new RecordHeaders();
                    byte[] payload = serializer.serialize(topic, headers, record);
                    lines.add(HexFormat.of().formatHex(headers.lastHeader("__value_schema_id").value()) + " " + HexFormat.of().formatHex(payload));
                }
            }
            Files.write(file, lines, StandardCharsets.UTF_8);
            return;
        }
        if ("guid-deserialize".equals(mode)) {
            try (KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer()) {
                deserializer.configure(config, false);
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) {
                        continue;
                    }
                    String[] parts = line.trim().split(" ");
                    Headers headers = new RecordHeaders();
                    headers.add("__value_schema_id", HexFormat.of().parseHex(parts[0]));
                    GenericRecord record = (GenericRecord) deserializer.deserialize(topic, headers, HexFormat.of().parseHex(parts[1]));
                    System.out.println(record.get("name") + "," + record.get("age"));
                }
            }
            return;
        }
        if ("serialize".equals(mode)) {
            Schema schema = new Schema.Parser().parse(SCHEMA);
            List<String> lines = new ArrayList<>();
            try (KafkaAvroSerializer serializer = new KafkaAvroSerializer()) {
                serializer.configure(config, false);
                for (int i = 0; i < Integer.parseInt(args[4]); i++) {
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("name", "java-" + i);
                    record.put("age", i);
                    lines.add(HexFormat.of().formatHex(serializer.serialize(topic, record)));
                }
            }
            Files.write(file, lines, StandardCharsets.UTF_8);
            return;
        }
        try (KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer()) {
            deserializer.configure(config, false);
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (line.isBlank()) {
                    continue;
                }
                GenericRecord record = (GenericRecord) deserializer.deserialize(topic, HexFormat.of().parseHex(line.trim()));
                System.out.println(record.get("name") + "," + record.get("age"));
            }
        }
    }
}
