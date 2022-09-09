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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

public final class AvroTypeUtil {

  private static Map<String, Conversion<?>> logicalTypeConverters = new HashMap<>();

  static {
    logicalTypeConverters.put(
        CqlLogicalTypes.CQL_DECIMAL, new CqlLogicalTypes.CqlDecimalConversion());
    logicalTypeConverters.put(
        CqlLogicalTypes.CQL_DURATION, new CqlLogicalTypes.CqlDurationLogicalType());
    logicalTypeConverters.put(
        CqlLogicalTypes.CQL_VARINT, new CqlLogicalTypes.CqlVarintConversion());
  }

  private AvroTypeUtil() {}

  public static boolean shouldWrapAvroType(GenericRecord record, Object fieldValue) {
    return record != null && record.getSchemaType() == SchemaType.AVRO && isMapOrList(fieldValue);
  }

  public static boolean shouldHandleLogicalType(GenericRecord record, Object fieldValue) {
    if (record == null && record.getSchemaType() != SchemaType.AVRO) {
      return false;
    }
    if (isVarint(fieldValue)) {
      return true;
    } else if (fieldValue instanceof GenericRecord
        && ((GenericRecord) fieldValue).getNativeObject()
            instanceof org.apache.avro.generic.GenericRecord) {
      org.apache.avro.generic.GenericRecord avroRecord =
          (org.apache.avro.generic.GenericRecord) ((GenericRecord) fieldValue).getNativeObject();
      return logicalTypeConverters.containsKey(avroRecord.getSchema().getName());
    }
    return false;
  }

  public static Object handleLogicalType(Object value) {
    if (isVarint(value)) {
      return logicalTypeConverters
          .get(CqlLogicalTypes.CQL_VARINT)
          .fromBytes((ByteBuffer) value, Schema.create(Schema.Type.BYTES), null)
          .toString();
    } else if (value instanceof GenericRecord) {
      org.apache.avro.generic.GenericRecord record =
          (org.apache.avro.generic.GenericRecord) ((GenericRecord) value).getNativeObject();
      return logicalTypeConverters
          .get(record.getSchema().getName())
          .fromRecord(record, record.getSchema(), record.getSchema().getLogicalType())
          .toString(); // this will utilize the StringToDuration & StringToBigDecimal
                       // ConvertingCodecs
    }

    throw new UnsupportedOperationException("cannot handle logical type for " + value);
  }

  private static boolean isMapOrList(Object mapOrList) {
    return mapOrList instanceof Map || mapOrList instanceof List;
  }

  private static boolean isVarint(Object varint) {
    return varint instanceof ByteBuffer;
  }
}
