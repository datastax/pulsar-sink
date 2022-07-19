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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.util.internal.JacksonUtils;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Test;

public class AvroContainerTypeRecordTest {
  @Test
  void should_populate_avro_schema_type() {
    assertEquals(
        SchemaType.AVRO,
        new AvroContainerTypeRecord(JacksonUtils.toJsonNode(Collections.emptyMap()))
            .getSchemaType());
  }

  @Test
  void should_throw_exception_for_primitive_types() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> new AvroContainerTypeRecord(IntNode.valueOf(10)));
  }

  @Test
  void should_throw_exception_for_null_native_object() {
    assertThrows(NullPointerException.class, () -> new AvroContainerTypeRecord(null));
  }

  @Test
  void should_populate_map_fields() {
    Map<String, Object> map =
        ImmutableMap.of(
            "bool",
            true,
            "float",
            1.2f,
            "bigint",
            new BigInteger("123"),
            "number",
            10,
            "string",
            "string");
    AvroContainerTypeRecord record = new AvroContainerTypeRecord(JacksonUtils.toJsonNode(map));
    assertNotNull(record.getFields());
    assertEquals(5, record.getFields().size());
    assertEquals(true, record.getField("bool"));
    assertEquals(1.2d, Math.round((double) record.getField("float")), 1);
    assertEquals(123L, record.getField("bigint"));
    assertEquals(10, record.getField("number"));
    assertEquals("string", record.getField("string"));
    assertNull(record.getField("null"));
  }

  @Test
  void should_populate_nested_map_fields() {
    Map<String, String> nestedMap = ImmutableMap.of("nestedk1", "nestedv1", "nestedk2", "nestedv2");
    Map<String, Object> map = ImmutableMap.of("k1", "v1", "k2", nestedMap);
    AvroContainerTypeRecord record = new AvroContainerTypeRecord(JacksonUtils.toJsonNode(map));
    assertNotNull(record.getFields());
    assertEquals(2, record.getFields().size());
    assertEquals("v1", record.getField("k1"));
    assertTrue(record.getField("k2") instanceof AvroContainerTypeRecord);
    AvroContainerTypeRecord nestedRecord = (AvroContainerTypeRecord) record.getField("k2");
    assertNotNull(nestedRecord.getFields());
    assertEquals(2, nestedRecord.getFields().size());
    assertEquals("nestedv1", nestedRecord.getField("nestedk1"));
    assertEquals("nestedv2", nestedRecord.getField("nestedk2"));
  }

  @Test
  void should_wrap_array_fields() {
    List<String> list = ImmutableList.of("f1", "f2");
    AvroContainerTypeRecord record = new AvroContainerTypeRecord(JacksonUtils.toJsonNode(list));
    assertNotNull(record.getFields());
    assertEquals(0, record.getFields().size());
    assertTrue(record.getNativeObject() instanceof JsonNode);
    JsonNode node = (JsonNode) record.getNativeObject();
    assertEquals("f1", node.get(0).asText());
    assertEquals("f2", node.get(1).asText());
  }

  @Test
  void should_populate_nested_array_fields() {
    List<String> nestedList = ImmutableList.of("nestedf1", "nestedf2");
    List<Object> list = ImmutableList.of("f1", nestedList);
    AvroContainerTypeRecord record = new AvroContainerTypeRecord(JacksonUtils.toJsonNode(list));
    assertEquals(0, record.getFields().size());
    assertTrue(record.getNativeObject() instanceof JsonNode);
    JsonNode node = (JsonNode) record.getNativeObject();
    assertEquals("f1", node.get(0).asText());
    JsonNode nestedNode = node.get(1);
    assertEquals("nestedf1", nestedNode.get(0).asText());
    assertEquals("nestedf2", nestedNode.get(1).asText());
  }
}
