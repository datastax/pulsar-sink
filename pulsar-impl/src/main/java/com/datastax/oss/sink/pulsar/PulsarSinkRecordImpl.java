/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.sink.pulsar;

import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.AbstractSinkRecord;
import com.datastax.oss.common.sink.AbstractSinkRecordHeader;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;

/** @author enrico.olivelli */
public class PulsarSinkRecordImpl implements AbstractSinkRecord {
  private final Record<?> record;
  private final LocalSchemaRegistry schemaRegistry;

  public PulsarSinkRecordImpl(Record<?> record, LocalSchemaRegistry schemaRegistry) {
    this.record = record;
    this.schemaRegistry = schemaRegistry;
  }

  @Override
  public Iterable<AbstractSinkRecordHeader> headers() {
    return record
        .getProperties()
        .entrySet()
        .stream()
        .map(entry -> new PulsarSinkRecordHeader(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  private static final class PulsarSinkRecordHeader implements AbstractSinkRecordHeader {
    private final String key;
    private final String value;

    public PulsarSinkRecordHeader(String key, String value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public String key() {
      return key;
    }

    @Override
    public Object value() {
      return value;
    }

    @Override
    public AbstractSchema schema() {
      return PulsarSchema.STRING;
    }

    @Override
    public String toString() {
      return "{" + "key=" + key + ", value=" + value + '}';
    }
  }

  @Override
  public Object key() {
    return record.getKey().orElse(null);
  }

  @Override
  public Object value() {
    if (record.getValue() instanceof GenericRecord) {
      return PulsarStruct.ofRecord((Record<GenericRecord>) record, schemaRegistry);
    } else {
      return record.getValue();
    }
  }

  @Override
  public Long timestamp() {
    return record.getEventTime().orElse(null);
  }

  @Override
  public String topic() {
    return shortTopic(record);
  }

  public Record<?> getRecord() {
    return record;
  }

  public static String shortTopic(Record<?> record) {
    // persistent://public/default/topicname
    // persistent://public/default/mytopic-partition-1
    if (!record.getTopicName().isPresent()) {
      return null;
    }
    TopicName topicName = TopicName.get(record.getTopicName().get());
    if (topicName.isPartitioned()) {
      // getPartitionedTopicName extract the topic name, handling the partition suffix
      topicName = TopicName.get(topicName.getPartitionedTopicName());
    }
    return topicName.getLocalName();
  }

  @Override
  public String toString() {
    return "PulsarSinkRecord{" + record + '}';
  }
}
