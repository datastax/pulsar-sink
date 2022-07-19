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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

/** Generic record for tests */
public class GenericRecordImpl implements GenericRecord {
  private final Map<String, Object> values;
  private final List<Field> fields;
  private final Object nativeObject;

  public GenericRecordImpl() {
    this(null);
  }

  public GenericRecordImpl(Object nativeObject) {
    this.values = new LinkedHashMap<>(); // we want predictable output for integration tests
    this.fields = new ArrayList<>();
    this.nativeObject = nativeObject;
  }

  public GenericRecordImpl put(String name, Object value) {
    if (nativeObject != null) {
      throw new IllegalStateException();
    }
    if (values.put(name, value) == null) {
      fields.add(new Field(name, values.size()));
    }
    return this;
  }

  @Override
  public byte[] getSchemaVersion() {
    return new byte[1];
  }

  @Override
  public List<Field> getFields() {
    return fields;
  }

  @Override
  public Object getField(String fieldName) {
    return values.get(fieldName);
  }

  @Override
  public Object getNativeObject() {
    return nativeObject;
  }

  @Override
  public SchemaType getSchemaType() {
    return SchemaType.AUTO_CONSUME;
  }

  @Override
  public String toString() {
    if (nativeObject != null) {
      return "GenericRecordImpl{" + "nativeObject=" + nativeObject + '}';
    } else {
      return "GenericRecordImpl{" + "values=" + values + '}';
    }
  }
}
