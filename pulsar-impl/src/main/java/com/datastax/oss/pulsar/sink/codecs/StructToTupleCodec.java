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

import com.datastax.oss.common.sink.AbstractField;
import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.record.StructDataMetadata;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.sink.pulsar.PulsarStruct;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Codec to convert a Pulsar Struct to a CQL Tuple. */
public class StructToTupleCodec extends ConvertingCodec<PulsarStruct, TupleValue> {

  private final ConvertingCodecFactory codecFactory;
  private final TupleType tupleType;

  StructToTupleCodec(ConvertingCodecFactory codecFactory, TupleType cqlType) {
    super(codecFactory.getCodecRegistry().codecFor(cqlType), PulsarStruct.class);
    this.codecFactory = codecFactory;
    this.tupleType = cqlType;
  }

  @Override
  public TupleValue externalToInternal(PulsarStruct external) {
    if (external == null) {
      return null;
    }

    List<DataType> componentTypes = tupleType.getComponentTypes();
    int size = componentTypes.size();
    AbstractSchema schema = external.schema();
    StructDataMetadata structMetadata = new StructDataMetadata(schema);

    Set<String> structFieldNames =
            schema.fields().stream().map(AbstractField::name).collect(Collectors.toSet());

    if (structFieldNames.size() != size) {
      throw new IllegalArgumentException(
              String.format("Expecting %d tuple fields, got %d", size, structFieldNames.size()));
    }

    TupleValue tupleValue = tupleType.newValue();

    // Tuple fields are named index_0, index_1, etc. following CDC convention
    for (int i = 0; i < size; i++) {
      String fieldName = "index_" + i;
      DataType componentType = componentTypes.get(i);

      // Check if field exists in the struct
      if (!schema.fields().stream().anyMatch(f -> f.name().equals(fieldName))) {
        throw new IllegalArgumentException(
            String.format(
                "Field %s not found in input struct for tuple component %d", fieldName, i));
      }

      @SuppressWarnings("unchecked")
      GenericType<Object> fieldType =
          (GenericType<Object>) structMetadata.getFieldType(fieldName, componentType);
      ConvertingCodec<Object, Object> fieldCodec =
          codecFactory.createConvertingCodec(componentType, fieldType, false);
      Object fieldValue = external.get(fieldName);
      Object convertedValue = fieldCodec.externalToInternal(fieldValue);
      tupleValue = tupleValue.set(i, convertedValue, fieldCodec.getInternalJavaType());
    }

    return tupleValue;
  }

  @Override
  public PulsarStruct internalToExternal(TupleValue internal) {
    if (internal == null) {
      return null;
    }
    throw new UnsupportedOperationException(
        "This codec does not support converting from TupleValue to Struct");
  }
}
