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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.AbstractSinkRecord;
import com.datastax.oss.common.sink.AbstractSinkRecordHeader;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;

/** @author enrico.olivelli */
public class PulsarSinkRecordImpl implements AbstractSinkRecord {
  private static final String PARTITIONED_TOPIC_SUFFIX = "-partition-";
  private final Record<?> record;
  private final Object key;
  private final Schema keySchema;
  private final Object value;
  private final Schema valueSchema;
  private final LocalSchemaRegistry schemaRegistry;

  public PulsarSinkRecordImpl(Record<?> record, LocalSchemaRegistry schemaRegistry) {
    this.record = record;
    this.schemaRegistry = schemaRegistry;
    Object payload = record.getValue();
    Schema<?> schema = record.getSchema();

    if (schema instanceof KeyValueSchema
        && payload instanceof GenericObject
        && ((GenericObject) payload).getNativeObject() instanceof KeyValue) {
      KeyValue kv = (KeyValue) ((GenericObject) payload).getNativeObject();
      KeyValueSchema kvSchema = (KeyValueSchema) schema;
      this.value = kv.getValue();
      this.key = kv.getKey();
      this.valueSchema = kvSchema.getValueSchema();
      this.keySchema = kvSchema.getKeySchema();
    } else {
      this.key = record.getKey().orElse(null);
      this.keySchema = Schema.STRING;
      this.valueSchema = schema;

      // unwrap simple schemas, wrapped by GenericObject
      if (payload instanceof GenericObject
          && ((((GenericObject) payload).getNativeObject() instanceof String)
              || (((GenericObject) payload).getNativeObject() instanceof byte[]))) {
        this.value = ((GenericObject) payload).getNativeObject();
      } else {
        this.value = payload;
      }
    }
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

  private Object unwrap(Object o, Schema schema) {
    if (o instanceof GenericRecord) {
      return PulsarStruct.ofRecord(
          (GenericRecord) o, schema, record.getTopicName(), record.getEventTime(), schemaRegistry);
    } else {
      if (o instanceof byte[]) {
        return new String((byte[]) o, UTF_8);
      } if (o instanceof ByteBuffer) {
        return new String(((ByteBuffer) o).array(), UTF_8);
      }else {
        return o;
      }
    }
  }

  @Override
  public Object key() {
    return unwrap(key, keySchema);
  }

  @Override
  public Object value() {
    return unwrap(value, valueSchema);
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
    // we cannot use org.apache.pulsar.common.naming.TopicName due
    // to classloading issues
    String rawName = record.getTopicName().get();
    int lastSlash = rawName.lastIndexOf('/');
    if (lastSlash < 0 || lastSlash == rawName.length()) {
      return rawName;
    }
    rawName = rawName.substring(lastSlash + 1);
    int pos = rawName.indexOf(PARTITIONED_TOPIC_SUFFIX);
    if (pos < 0) {
      return rawName;
    } else {
      return rawName.substring(0, pos);
    }
  }

  @Override
  public String toString() {
    return "PulsarSinkRecord{" + record + '}';
  }
}
