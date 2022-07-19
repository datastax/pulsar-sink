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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Test;

public class AvroTypeUtilTest {

  @Test
  void should_return_false_for_null() {
    assertFalse(AvroTypeUtil.shouldWrapAvroType(null, "whatever"));
  }

  @Test
  void should_return_false_for_non_avro() {
    GenericRecordImpl r =
        new GenericRecordImpl() {
          @Override
          public SchemaType getSchemaType() {
            return SchemaType.JSON;
          }
        };
    assertFalse(AvroTypeUtil.shouldWrapAvroType(r, "whatever"));
  }

  @Test
  void should_return_false_for_not_implemented_schema_type() {
    GenericRecordImpl r = new GenericRecordImpl();
    assertFalse(AvroTypeUtil.shouldWrapAvroType(r, "whatever"));
  }

  @Test
  void should_return_false_for_primitive_avro_type() {
    RecordSchemaBuilder builder = SchemaBuilder.record("type");
    builder.field("test").type(SchemaType.INT32);
    GenericSchema<GenericRecord> avro =
        org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));

    assertFalse(
        AvroTypeUtil.shouldWrapAvroType(avro.newRecordBuilder().set("test", 7).build(), "test"));
  }

  @Test
  void should_return_false_for_array_avro_type() {
    MyPojo pojo = new MyPojo();
    pojo.setName("name");
    pojo.setMap(ImmutableMap.of("v1", "v2"));
    Schema avroSchema = Schema.AVRO(MyPojo.class);
    byte[] bytes = avroSchema.encode(pojo);
    GenericAvroRecord record =
        (GenericAvroRecord) GenericAvroSchema.of(avroSchema.getSchemaInfo()).decode(bytes);
    assertTrue(AvroTypeUtil.shouldWrapAvroType(record, record.getField("map")));
  }

  @Test
  void should_return_false_for_map_avro_type() {
    MyPojo pojo = new MyPojo();
    pojo.setName("name");
    pojo.setList(ImmutableList.of("v1"));
    Schema avroSchema = Schema.AVRO(MyPojo.class);
    byte[] bytes = avroSchema.encode(pojo);
    GenericAvroRecord record =
        (GenericAvroRecord) GenericAvroSchema.of(avroSchema.getSchemaInfo()).decode(bytes);
    assertTrue(AvroTypeUtil.shouldWrapAvroType(record, record.getField("list")));
  }
}
