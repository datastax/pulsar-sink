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
package com.datastax.oss.pulsar.sink.codecs;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import com.datastax.oss.sink.pulsar.GenericRecordImpl;
import com.datastax.oss.sink.pulsar.LocalSchemaRegistry;
import com.datastax.oss.sink.pulsar.PulsarRecordImpl;
import com.datastax.oss.sink.pulsar.PulsarStruct;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;

public class StructToTupleCodecTest {
  private final TupleType tupleType = DataTypes.tupleOf(DataTypes.INT, DataTypes.DOUBLE);

  private final TupleValue tupleValue = tupleType.newValue().setInt(0, 42).setDouble(1, 0.12d);

  private final StructToTupleCodec tupleCodec =
      (StructToTupleCodec)
          new ConvertingCodecFactory(new TextConversionContext())
              .<PulsarStruct, TupleValue>createConvertingCodec(
                  tupleType, GenericType.of(PulsarStruct.class), true);

  private Schema schema;
  private Record<GenericRecord> record;
  private GenericRecordImpl struct;

  {
    RecordSchemaBuilder builder = SchemaBuilder.record("MyBean");
    builder.field("index_0").type(SchemaType.INT32);
    builder.field("index_1").type(SchemaType.DOUBLE);
    schema = Schema.generic(builder.build(SchemaType.AVRO));

    struct = new GenericRecordImpl().put("index_0", 42).put("index_1", 0.12d);

    record = new PulsarRecordImpl(null, null, struct, schema);
  }

  private final LocalSchemaRegistry registry = new LocalSchemaRegistry();

  @Test
  void should_convert_from_valid_external() {
    assertThat(tupleCodec)
        .convertsFromExternal(PulsarStruct.ofRecord(record, registry))
        .toInternal(tupleValue);
  }

  @Test
  void should_not_convert_from_invalid_external() {

    // Missing index_1
    RecordSchemaBuilder builder1 = SchemaBuilder.record("MyBean");
    builder1.field("index_0").type(SchemaType.INT32);
    Schema schemaOther1 = Schema.generic(builder1.build(SchemaType.AVRO));

    Record<GenericRecord> other1 =
        new PulsarRecordImpl(null, null, new GenericRecordImpl().put("index_0", 42), schemaOther1);

    // Extra field index_2
    RecordSchemaBuilder builder2 = SchemaBuilder.record("MyBean");
    builder2.field("index_0").type(SchemaType.INT32);
    builder2.field("index_1").type(SchemaType.DOUBLE);
    builder2.field("index_2").type(SchemaType.INT32);
    Schema schemaOther2 = Schema.generic(builder2.build(SchemaType.AVRO));

    Record<GenericRecord> other2 =
        new PulsarRecordImpl(
            null,
            null,
            new GenericRecordImpl().put("index_0", 42).put("index_1", 0.12d).put("index_2", 7),
            schemaOther2);

    // Wrong field name
    RecordSchemaBuilder builder3 = SchemaBuilder.record("MyBean");
    builder3.field("f1").type(SchemaType.INT32);
    builder3.field("f2").type(SchemaType.DOUBLE);
    Schema schemaOther3 = Schema.generic(builder3.build(SchemaType.AVRO));

    Record<GenericRecord> other3 =
        new PulsarRecordImpl(
            null, null, new GenericRecordImpl().put("f1", 42).put("f2", 0.12d), schemaOther3);

    assertThat(tupleCodec)
        .cannotConvertFromExternal(PulsarStruct.ofRecord(other1, registry))
        .cannotConvertFromExternal(PulsarStruct.ofRecord(other2, registry))
        .cannotConvertFromExternal(PulsarStruct.ofRecord(other3, registry));
  }
}
