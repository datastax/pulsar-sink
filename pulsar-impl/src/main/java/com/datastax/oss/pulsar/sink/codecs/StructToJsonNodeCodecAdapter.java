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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.json.JsonNodeConvertingCodec;
import com.datastax.oss.sink.pulsar.PulsarStruct;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;

/**
 * A generic codec adapter to convert a Pulsar Struct wrapping JSON objets to any sql type. It
 * leverages the {@link JsonNodeConvertingCodec} and all its derivatives.
 */
public class StructToJsonNodeCodecAdapter<T> extends ConvertingCodec<PulsarStruct, T> {

  private final ConvertingCodecFactory codecFactory;
  private final DataType definition;

  private ConvertingCodec<JsonNode, T> jsonNodeCodec;

  StructToJsonNodeCodecAdapter(ConvertingCodecFactory codecFactory, DataType cqlType) {
    super(codecFactory.getCodecRegistry().codecFor(cqlType), PulsarStruct.class);
    this.codecFactory = codecFactory;
    this.definition = cqlType;
    this.jsonNodeCodec =
        codecFactory.createConvertingCodec(this.definition, GenericType.of(JsonNode.class), true);
  }

  @Override
  public T externalToInternal(PulsarStruct external) {
    if (external == null) {
      return null;
    } else if (external.getRecord().getNativeObject() instanceof JsonNode) {
      return this.jsonNodeCodec.externalToInternal(
          (JsonNode) external.getRecord().getNativeObject());
    }

    Object nativeObject = external.getRecord().getNativeObject();
    throw new IllegalArgumentException(
        "Expecting JsonNode, got "
            + (nativeObject == null ? "NULL" : nativeObject.getClass().getName()));
  }

  @Override
  public PulsarStruct internalToExternal(T internal) {
    throw new UnsupportedOperationException(
        "This codec does not support converting to PulsarStruct");
  }
}
