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

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/**
 * This is a local cache of PulsarSchema. Unfortunately Pulsar does not return information about the
 * datatype of Fields, so we have to understand it using Java reflection.
 */
public class LocalSchemaRegistry {

  private final ConcurrentHashMap<String, PulsarSchema> registry = new ConcurrentHashMap<>();

  public PulsarSchema ensureAndUpdateSchemaFromStruct(Record<?> struct) {
    String path = computeRecordSchemaPath(struct);
    // for nested structures we are going to add the name of the field
    return ensureAndUpdateSchema(path, struct.getValue());
  }

  public static String computeRecordSchemaPath(Record struct) {
    return computeRecordSchemaPath(struct.getSchema(), struct.getTopicName());
  }

  public static String computeRecordSchemaPath(Schema schema, Optional<String> topicName) {
    String schemaDef = "?";
    // versions of Pulsar prior to 2.6.3 do not report schema information
    if (schema != null && schema.getSchemaInfo() != null) {
      schemaDef = schema.getSchemaInfo().getSchemaDefinition();
    } // we using as key the fully qualified topic name + string (JSON) representation of the schema
    // this way we are supporting schema evolution easily
    String path = topicName.orElse(null) + "_" + schemaDef;
    return path;
  }

  public PulsarSchema ensureAndUpdateSchema(String path, Object value) {
    if (value instanceof GenericRecord) {
      GenericRecord struct = (GenericRecord) value;
      PulsarSchema res = registry.get(path);
      if (res == null) {
        res = PulsarSchema.createFromStruct(path, struct, this);
        registry.put(path, res);
      }
      // need to recover nulls from previous records
      res.update(path, struct, this);
      return res;
    } else {
      // primitive values
      return registry.computeIfAbsent(path, s -> PulsarSchema.of(path, value, this));
    }
  }

  PulsarSchema getAtPath(String path) {
    return registry.get(path);
  }
}
