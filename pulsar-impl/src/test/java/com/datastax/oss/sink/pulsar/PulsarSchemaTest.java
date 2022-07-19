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

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.common.sink.AbstractSchema;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;

/** @author enrico.olivelli */
public class PulsarSchemaTest {

  @Test
  public void testOf() {
    LocalSchemaRegistry registry = null;
    assertSame(PulsarSchema.STRING, PulsarSchema.of("the-path", "string", registry));
    assertSame(PulsarSchema.BOOLEAN, PulsarSchema.of("the-path", true, registry));

    assertSame(PulsarSchema.FLOAT32, PulsarSchema.of("the-path", 1f, registry));

    assertSame(PulsarSchema.FLOAT64, PulsarSchema.of("the-path", 1d, registry));

    assertSame(PulsarSchema.INT16, PulsarSchema.of("the-path", (short) 1, registry));

    assertSame(PulsarSchema.INT8, PulsarSchema.of("the-path", (byte) 1, registry));

    assertSame(PulsarSchema.BYTES, PulsarSchema.of("the-path", new byte[0], registry));

    assertSame(PulsarSchema.FIRST_VALUE_NULL, PulsarSchema.of("the-path", null, registry));
  }

  @Test
  public void testGenericRecordAndSchemaEvolution() {
    LocalSchemaRegistry registry = new LocalSchemaRegistry();

    // initially we see a null value for "nullvalue"
    GenericRecordImpl genericRecord = new GenericRecordImpl().put("test", 1).put("nullvalue", null);
    PulsarSchema schema = PulsarSchema.of("path", genericRecord, registry);
    assertSame(schema, PulsarSchema.of("path", genericRecord, registry));
    assertSame(PulsarSchema.STRING, schema.keySchema());
    assertSame(schema, schema.valueSchema());
    assertEquals(2, schema.fields().size());
    assertSame(PulsarSchema.INT32, schema.field("test").schema());
    // let's assume it is a STRING
    assertSame(PulsarSchema.FIRST_VALUE_NULL, schema.field("nullvalue").schema());

    // then we get to know that it is a "double"
    GenericRecordImpl genericRecordNotNull =
        new GenericRecordImpl().put("test", 1).put("nullvalue", 1d);
    PulsarSchema schemaUpdated = PulsarSchema.of("path", genericRecordNotNull, registry);
    assertSame(schemaUpdated, schema);

    assertEquals(2, schema.fields().size());
    assertSame(PulsarSchema.INT32, schema.field("test").schema());
    assertSame(PulsarSchema.FLOAT64, schema.field("nullvalue").schema());

    // adding a new field (this is a valid schema change)
    GenericRecordImpl genericRecordWithNewField =
        new GenericRecordImpl().put("test", 1).put("nullvalue", 1d).put("schemaevolution", true);
    PulsarSchema schemaUpdatedAgain = PulsarSchema.of("path", genericRecordWithNewField, registry);
    assertSame(schemaUpdatedAgain, schema);

    assertEquals(3, schema.fields().size());
    assertSame(PulsarSchema.INT32, schema.field("test").schema());
    assertSame(PulsarSchema.FLOAT64, schema.field("nullvalue").schema());
    assertSame(PulsarSchema.BOOLEAN, schema.field("schemaevolution").schema());
  }

  @Test
  public void testWithPulsarStruct() {
    LocalSchemaRegistry registry = new LocalSchemaRegistry();

    GenericRecordImpl genericRecord = new GenericRecordImpl().put("test", 1);
    RecordSchemaBuilder builder = SchemaBuilder.record("type");
    builder.field("test").type(SchemaType.INT32);
    GenericSchema<GenericRecord> genericSchema =
        org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/topic", null, genericRecord, genericSchema);
    PulsarSchema schema = registry.ensureAndUpdateSchemaFromStruct(record);
    PulsarStruct struct = PulsarStruct.ofRecord(record, registry);
    assertSame(schema, struct.schema());
    assertSame(PulsarSchema.STRING, schema.keySchema());
    assertSame(schema, schema.valueSchema());
    assertEquals(1, schema.fields().size());
    assertSame("test", schema.field("test").name());
    assertSame(PulsarSchema.INT32, schema.field("test").schema());
  }

  @Test
  public void testWithNestedPulsarStruct() {
    LocalSchemaRegistry registry = new LocalSchemaRegistry();

    GenericRecordImpl genericRecord =
        new GenericRecordImpl()
            .put("test", 1)
            .put("testNestedStruct", new GenericRecordImpl().put("testnested", 1L))
            .put("testNestedStruct2", new GenericRecordImpl().put("testnested", 1L));

    RecordSchemaBuilder builderNested = SchemaBuilder.record("typeNested");
    builderNested.field("testnested").type(SchemaType.INT64);
    GenericSchema<GenericRecord> genericSchemaNested =
        org.apache.pulsar.client.api.Schema.generic(builderNested.build(SchemaType.AVRO));

    RecordSchemaBuilder builder = SchemaBuilder.record("type");
    builder.field("test").type(SchemaType.INT32);
    builder.field("testNestedStruct", genericSchemaNested).type(SchemaType.AVRO);
    builder.field("testNestedStruct2", genericSchemaNested).type(SchemaType.AVRO);
    GenericSchema<GenericRecord> genericSchema =
        org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/topic", null, genericRecord, genericSchema);
    PulsarSchema schema = registry.ensureAndUpdateSchemaFromStruct(record);
    PulsarStruct struct = PulsarStruct.ofRecord(record, registry);
    assertSame(schema, struct.schema());
    assertSame(PulsarSchema.STRING, schema.keySchema());
    assertSame(schema, schema.valueSchema());
    assertEquals(3, schema.fields().size());
    assertSame("test", schema.field("test").name());
    assertSame(PulsarSchema.INT32, schema.field("test").schema());

    assertSame("testNestedStruct", schema.field("testNestedStruct").name());
    PulsarSchema nestedStructSchema = (PulsarSchema) schema.field("testNestedStruct").schema();

    assertSame("testNestedStruct2", schema.field("testNestedStruct2").name());
    PulsarSchema nestedStructSchema2 = (PulsarSchema) schema.field("testNestedStruct2").schema();

    assertNotSame(nestedStructSchema, nestedStructSchema2);

    PulsarSchema nestedStructSchemaByPath =
        registry.getAtPath(
            "persistent://tenant/namespace/topic_{\"type\":\"record\",\"name\":\"type\",\"fields\":[{\"name\":\"test\",\"type\":\"int\"},{\"name\":\"testNestedStruct\",\"type\":{\"type\":\"record\",\"name\":\"typeNested\",\"fields\":[{\"name\":\"testnested\",\"type\":\"long\"}]}},{\"name\":\"testNestedStruct2\",\"type\":\"typeNested\"}]}.testNestedStruct");
    assertSame(nestedStructSchemaByPath, nestedStructSchema);
  }

  @Test
  public void testWithAvroMapAndList() {
    LocalSchemaRegistry registry = new LocalSchemaRegistry();

    MyPojo pojo = new MyPojo();
    pojo.setName("name");
    pojo.setMap(ImmutableMap.of("v1", "v2"));
    pojo.setList(ImmutableList.of("l1", "l2"));
    Schema avroSchema = Schema.AVRO(MyPojo.class);
    byte[] bytes = avroSchema.encode(pojo);
    GenericAvroRecord avro =
        (GenericAvroRecord) GenericAvroSchema.of(avroSchema.getSchemaInfo()).decode(bytes);
    Record<GenericRecord> record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", "{\"pk\":15}", avro, avroSchema);
    PulsarSchema schema = registry.ensureAndUpdateSchemaFromStruct(record);
    PulsarStruct struct = PulsarStruct.ofRecord(record, registry);

    assertSame(schema, struct.schema());
    assertSame(PulsarSchema.STRING, schema.keySchema());
    assertSame(schema, schema.valueSchema());
    assertEquals(3, schema.fields().size());
    assertSame(PulsarSchema.STRING, schema.field("name").schema());
    assertSame(AbstractSchema.Type.STRUCT, schema.field("map").schema().type());
    assertSame(AbstractSchema.Type.STRUCT, schema.field("list").schema().type());

    assertTrue(struct.get("map") instanceof PulsarStruct);
    assertTrue(struct.get("list") instanceof PulsarStruct);

    PulsarStruct mapStruct = (PulsarStruct) struct.get("map");
    PulsarStruct listStruct = (PulsarStruct) struct.get("list");

    assertTrue(mapStruct.getRecord().getNativeObject() instanceof JsonNode);
    assertTrue(listStruct.getRecord().getNativeObject() instanceof JsonNode);

    JsonNode mapNode = (JsonNode) mapStruct.getRecord().getNativeObject();
    JsonNode listNode = (JsonNode) listStruct.getRecord().getNativeObject();

    ObjectMapper mapper = new ObjectMapper();
    Map<String, String> convertedMap =
        mapper.convertValue(mapNode, new TypeReference<Map<String, String>>() {});
    List<String> convertedList =
        mapper.convertValue(listNode, new TypeReference<List<String>>() {});

    assertEquals(pojo.getMap(), convertedMap);
    assertEquals(pojo.getList(), convertedList);
  }
}
