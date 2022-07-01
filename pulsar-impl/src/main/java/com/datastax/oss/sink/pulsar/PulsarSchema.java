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

import com.datastax.oss.common.sink.AbstractField;
import com.datastax.oss.common.sink.AbstractSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Datatype. */
public class PulsarSchema implements AbstractSchema {

  public static final PulsarSchema INT8 = new PulsarSchema(AbstractSchema.Type.INT8);
  public static final PulsarSchema INT16 = new PulsarSchema(AbstractSchema.Type.INT16);
  public static final PulsarSchema INT32 = new PulsarSchema(AbstractSchema.Type.INT32);
  public static final PulsarSchema INT64 = new PulsarSchema(AbstractSchema.Type.INT64);
  public static final PulsarSchema FLOAT32 = new PulsarSchema(AbstractSchema.Type.FLOAT32);
  public static final PulsarSchema FLOAT64 = new PulsarSchema(AbstractSchema.Type.FLOAT64);
  public static final PulsarSchema BOOLEAN = new PulsarSchema(AbstractSchema.Type.BOOLEAN);
  public static final PulsarSchema STRING = new PulsarSchema(AbstractSchema.Type.STRING);
  public static final PulsarSchema BYTES = new PulsarSchema(AbstractSchema.Type.BYTES);
  public static final PulsarSchema FIRST_VALUE_NULL = new PulsarSchema(Type.STRING, false);

  public static PulsarSchema of(String path, Object value, LocalSchemaRegistry registry) {
    if (value == null) {
      // there is no support for NULLS in Cassandra Driver type system
      // using STRING
      return FIRST_VALUE_NULL;
    }
    if (value instanceof Integer) {
      return INT32;
    }
    if (value instanceof String) {
      return STRING;
    }
    if (value instanceof Long) {
      return INT64;
    }
    if (value instanceof byte[]) {
      return BYTES;
    }
    if (value instanceof Boolean) {
      return BOOLEAN;
    }
    if (value instanceof Byte) {
      return INT8;
    }
    if (value instanceof Float) {
      return FLOAT32;
    }
    if (value instanceof Double) {
      return FLOAT64;
    }
    if (value instanceof Short) {
      return INT16;
    }
    if (value instanceof GenericRecord) {
      return registry.ensureAndUpdateSchema(path, (GenericRecord) value);
    }
    if (value instanceof PulsarStruct) {
      PulsarStruct struct = (PulsarStruct) value;
      return registry.ensureAndUpdateSchema(path, struct.getRecord());
    }
    throw new UnsupportedOperationException("type " + value.getClass());
  }

  static PulsarSchema createFromStruct(
      String path, GenericRecord value, LocalSchemaRegistry registry) {
    return new PulsarSchema(path, value, registry);
  }

  private final Map<String, PulsarField> fields;
  private final AbstractSchema.Type type;
  private final boolean valueWasNull;

  private PulsarSchema(String path, GenericRecord template, LocalSchemaRegistry registry) {
    this.fields = new ConcurrentHashMap<>();
    this.type = AbstractSchema.Type.STRUCT;
    this.valueWasNull = true;
    update(path, template, registry);
  }

  private PulsarSchema(AbstractSchema.Type type) {
    this(type, true);
  }

  private PulsarSchema(AbstractSchema.Type type, boolean valueWasNull) {
    this.type = type;
    this.fields = Collections.emptyMap();
    this.valueWasNull = valueWasNull;
  }

  private static final Logger log = LoggerFactory.getLogger(PulsarSchema.class);

  /**
   * Unfortunately Pulsar does not return information about the datatype of Fields, so we have to
   * understand it using Java reflection.
   *
   * @param path context definition for the schema (root record or subfield)
   * @param template an instance.
   */
  public final void update(String path, GenericRecord template, LocalSchemaRegistry registry) {
    for (Field f : template.getFields()) {
      Object value = template.getField(f);
      // in case of null value we are going to use
      // a dummy (string) type
      // at the first occourrance of a non-null value
      // we are updating the type
      // it is not expected that a field changes data type
      // once we find a non-null value
      PulsarSchema schemaForField = PulsarSchema.of(path + "." + f.getName(), value, registry);

      if (fields.containsKey(f.getName()) && !schemaForField.isValueWasNull()) {
        if (log.isDebugEnabled()) {
          log.debug("skipping updating field {} schema because it was a NULL value");
        }
        continue;
      }
      PulsarField field = new PulsarField(f.getName(), schemaForField);
      this.fields.put(f.getName(), field);
    }
  }

  @Override
  public AbstractSchema valueSchema() {
    return this;
  }

  @Override
  public AbstractSchema keySchema() {
    // in Pulsar we only have String keys
    return STRING;
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public List<? extends AbstractField> fields() {
    return new ArrayList<>(fields.values());
  }

  @Override
  public AbstractField field(String name) {
    return fields.get(name);
  }

  @Override
  public String toString() {
    return "PulsarSchema{"
        + "fields="
        + fields
        + ", type="
        + type
        + ", valueWasNull="
        + valueWasNull
        + '}';
  }

  public boolean isValueWasNull() {
    return valueWasNull;
  }
}
