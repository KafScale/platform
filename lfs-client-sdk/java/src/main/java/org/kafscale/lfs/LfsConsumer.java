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

package org.kafscale.lfs;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class LfsConsumer {
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final LfsResolver resolver;

    public LfsConsumer(KafkaConsumer<byte[], byte[]> consumer, LfsResolver resolver) {
        this.consumer = consumer;
        this.resolver = resolver;
    }

    public List<LfsResolver.ResolvedRecord> pollResolved(Duration timeout) throws Exception {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
        List<LfsResolver.ResolvedRecord> out = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> rec : records) {
            out.add(resolver.resolve(rec.value()));
        }
        return out;
    }
}
