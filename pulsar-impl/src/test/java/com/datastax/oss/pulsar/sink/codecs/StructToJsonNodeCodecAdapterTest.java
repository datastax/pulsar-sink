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

import com.datastax.oss.driver.api.core.type.*;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.DefaultListType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import com.datastax.oss.sink.pulsar.GenericRecordImpl;
import com.datastax.oss.sink.pulsar.LocalSchemaRegistry;
import com.datastax.oss.sink.pulsar.PulsarRecordImpl;
import com.datastax.oss.sink.pulsar.PulsarStruct;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;

class StructToJsonNodeCodecAdapterTest {
  private final PrimitiveType textPrimitiveType = new PrimitiveType(13);
  private final MapType mapType = new DefaultMapType(textPrimitiveType, textPrimitiveType, false);

  private final ListType listType = new DefaultListType(textPrimitiveType, false);

  private final Map<String, String> mapValue = ImmutableMap.of("k1", "v1", "k2", "v2");

  private final List<String> listValue = ImmutableList.of("l1", "l2");
  private final StructToJsonNodeCodecAdapter structMapCodec =
      (StructToJsonNodeCodecAdapter)
          new ConvertingCodecFactory(new TextConversionContext())
              .<PulsarStruct, Map<String, String>>createConvertingCodec(
                  mapType, GenericType.of(PulsarStruct.class), true);

  private final StructToJsonNodeCodecAdapter structListCodec =
      (StructToJsonNodeCodecAdapter)
          new ConvertingCodecFactory(new TextConversionContext())
              .<PulsarStruct, List<String>>createConvertingCodec(
                  listType, GenericType.of(PulsarStruct.class), true);

  private Schema schema;
  private Record<GenericRecord> record;
  private GenericRecordImpl struct;

  private ObjectMapper mapper = new ObjectMapper();

  private final LocalSchemaRegistry registry = new LocalSchemaRegistry();

  @Test
  void should_convert_map_from_valid_external() {
    JsonNode nativeObject = mapper.valueToTree(mapValue);
    struct = new GenericRecordImpl(nativeObject);
    record = new PulsarRecordImpl(null, null, struct, schema);

    assertThat(structMapCodec)
        .convertsFromExternal(PulsarStruct.ofRecord(record, registry))
        .toInternal(mapValue)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_list_from_valid_external() {
    JsonNode nativeObject = mapper.valueToTree(listValue);
    struct = new GenericRecordImpl(nativeObject);
    record = new PulsarRecordImpl(null, null, struct, schema);

    assertThat(structListCodec)
        .convertsFromExternal(PulsarStruct.ofRecord(record, registry))
        .toInternal(listValue)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_not_convert_map_from_invalid_external() throws Exception {
    assertThat(structMapCodec)
        .cannotConvertFromExternal(mapper.readTree("{\"not a pulsar struct!\":\"foo\"}"));
  }

  @Test
  void should_not_convert_list_from_invalid_external() throws Exception {
    assertThat(structListCodec)
        .cannotConvertFromExternal(mapper.readTree("[\"not a pulsar struct!\"]"));
  }

  @Test
  void should_not_convert_list_from_null_internal() {
    assertThat(structListCodec).cannotConvertFromInternal(null);
  }

  @Test
  void should_not_convert_map_from_null_internal() {
    assertThat(structMapCodec).cannotConvertFromInternal(null);
  }

  @Test
  void should_not_convert_list_from_internal() {
    assertThat(structMapCodec).cannotConvertFromInternal(listValue);
  }

  @Test
  void should_not_convert_map_from_internal() {
    assertThat(structMapCodec).cannotConvertFromInternal(mapValue);
  }

  @Test
  void should_not_convert_list_from_null_native_external() {
    struct = new GenericRecordImpl(null);
    record = new PulsarRecordImpl(null, null, struct, schema);

    assertThat(structListCodec).cannotConvertFromExternal(PulsarStruct.ofRecord(record, registry));
  }

  @Test
  void should_not_convert_map_from_null_native_external() {
    struct = new GenericRecordImpl(null);
    record = new PulsarRecordImpl(null, null, struct, schema);

    assertThat(structMapCodec).cannotConvertFromExternal(PulsarStruct.ofRecord(record, registry));
  }

  @Test
  void should_not_convert_list_from_invalid_native_external() {
    Long nativeObject = 0L;
    struct = new GenericRecordImpl(nativeObject);
    record = new PulsarRecordImpl(null, null, struct, schema);

    assertThat(structListCodec).cannotConvertFromExternal(PulsarStruct.ofRecord(record, registry));
  }

  @Test
  void should_not_convert_map_from_invalid_native_external() {
    Long nativeObject = 0L;
    struct = new GenericRecordImpl(nativeObject);
    record = new PulsarRecordImpl(null, null, struct, schema);

    assertThat(structMapCodec).cannotConvertFromExternal(PulsarStruct.ofRecord(record, registry));
  }
}
