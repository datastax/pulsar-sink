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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

/** Wraps Avro container types (i.e. Maps and Lists) in a generic record * */
public class AvroContainerTypeRecord implements GenericRecord {
  private final List<Field> fields;
  private final Object nativeObject;

  public AvroContainerTypeRecord(JsonNode nativeObject) {
    if (nativeObject == null) throw new NullPointerException("nativeObject cannot be null!");
    if (!nativeObject.isContainerNode())
      throw new UnsupportedOperationException("nativeObject must be a container node!");

    this.fields = populateFields(nativeObject);
    this.nativeObject = nativeObject;
  }

  private AvroContainerTypeRecord(JsonNode nativeObject, List<Field> fields) {
    this.fields = fields;
    this.nativeObject = nativeObject;
  }

  @Override
  public byte[] getSchemaVersion() {
    throw new UnsupportedOperationException("GenericRecordWrapper does not support schema version");
  }

  @Override
  public List<Field> getFields() {
    return fields;
  }

  /** Ported from org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord */
  @Override
  public Object getField(String fieldName) {
    JsonNode fn = ((JsonNode) this.nativeObject).get(fieldName);
    if (fn == null) {
      return null;
    }
    if (fn.isContainerNode()) {
      List<Field> fields = populateFields(fn);
      return new AvroContainerTypeRecord(fn, fields);
    } else if (fn.isBoolean()) {
      return fn.asBoolean();
    } else if (fn.isFloatingPointNumber()) {
      return fn.asDouble();
    } else if (fn.isBigInteger()) {
      if (fn.canConvertToLong()) {
        return fn.asLong();
      } else {
        return fn.asText();
      }
    } else if (fn.isNumber()) {
      return fn.numberValue();
    } else if (fn.isBinary()) {
      try {
        return fn.binaryValue();
      } catch (IOException e) {
        return fn.asText();
      }
    } else if (fn.isNull()) {
      return null;
    } else {
      return fn.asText();
    }
  }

  @Override
  public Object getNativeObject() {
    return nativeObject;
  }

  @Override
  public SchemaType getSchemaType() {
    return SchemaType.AVRO;
  }

  @Override
  public String toString() {
    return "AvroContainerTypeRecordWrapper{" + "nativeObject=" + nativeObject + "}";
  }

  private List<Field> populateFields(JsonNode node) {
    AtomicInteger idx = new AtomicInteger(0);
    return (List)
        Lists.newArrayList(node.fieldNames())
            .stream()
            .map((f) -> new Field(f, idx.getAndIncrement()))
            .collect(Collectors.toList());
  }
}
