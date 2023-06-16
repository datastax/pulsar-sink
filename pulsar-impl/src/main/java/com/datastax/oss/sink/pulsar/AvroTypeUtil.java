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

import static com.datastax.oss.sink.pulsar.CqlLogicalTypes.CQL_DECIMAL;
import static com.datastax.oss.sink.pulsar.CqlLogicalTypes.CQL_DURATION;
import static com.datastax.oss.sink.pulsar.CqlLogicalTypes.CQL_VARINT;
import static com.datastax.oss.sink.pulsar.CqlLogicalTypes.DATE;
import static com.datastax.oss.sink.pulsar.CqlLogicalTypes.TIMESTAMP_MILLIS;
import static com.datastax.oss.sink.pulsar.CqlLogicalTypes.TIME_MICROS;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

public final class AvroTypeUtil {

  private static Map<String, Conversion<?>> logicalTypeConverters = new HashMap<>();
  private static boolean decodeCDCDataTypes;

  private AvroTypeUtil() {}

  public static boolean shouldWrapAvroType(GenericRecord record, Object fieldValue) {
    return record != null && record.getSchemaType() == SchemaType.AVRO && isMapOrList(fieldValue);
  }

  public static boolean shouldHandleCassandraCDCLogicalType(
      GenericRecord record, String fieldName) {
    if (!decodeCDCDataTypes || (record == null && record.getSchemaType() != SchemaType.AVRO)) {
      return false;
    }
    return getLogicalType(record, fieldName)
        .map(logicalType -> logicalTypeConverters.containsKey(logicalType.getName()))
        .orElse(false);
  }

  public static boolean shouldHandleCassandraCDCLogicalType(Schema schema) {
    if (!decodeCDCDataTypes) {
      return false;
    }
    return getLogicalType(schema)
        .map(logicalType -> logicalTypeConverters.containsKey(logicalType.getName()))
        .orElse(false);
  }

  /**
   * Handles logical types that originate from an upstream C* CDC only.
   *
   * @return string representation of the logical type to leverage {@link
   *     com.datastax.oss.dsbulk.codecs.api.ConvertingCodec}
   * @see <a href="Supported Cassandra Data
   *     Structures">https://docs.datastax.com/en/cdc-for-cassandra/cdc-apache-cassandra/2.2.0/index.html#_supported_cassandra_data_structures</a>
   */
  public static Object handleCassandraCDCLogicalType(
      GenericRecord record, String fieldName, Object fieldValue) {
    if (!decodeCDCDataTypes) {
      throw new IllegalStateException(
          "cannot handle CDC logical types because the decodeCDCDataTypes config is false");
    }
    if (fieldValue == null) {
      return null; // logical types are optional
    }
    return getLogicalType(record, fieldName)
        .map(
            logicalType -> {
              if (isVarint(logicalType)) {
                return logicalTypeConverters
                    .get(CQL_VARINT)
                    .fromBytes((ByteBuffer) fieldValue, null, null)
                    .toString();
              } else if (isDate(logicalType)) {
                return logicalTypeConverters
                    .get(DATE)
                    .fromInt((int) fieldValue, null, null)
                    .toString();
              } else if (isTimeMicros(logicalType)) {
                return logicalTypeConverters
                    .get(TIME_MICROS)
                    .fromLong((long) fieldValue, null, null)
                    .toString();
              } else if (isTimestampMillis(logicalType)) {
                return logicalTypeConverters
                    .get(TIMESTAMP_MILLIS)
                    .fromLong((long) fieldValue, null, null)
                    .toString();
              }
              return fieldValue instanceof GenericRecord
                      && ((GenericRecord) fieldValue).getNativeObject() instanceof IndexedRecord
                      && logicalTypeConverters.containsKey(logicalType.getName())
                  ? logicalTypeConverters
                      .get(logicalType.getName())
                      .fromRecord(
                          (IndexedRecord) ((GenericRecord) fieldValue).getNativeObject(),
                          null,
                          null)
                      .toString() // this will utilize the StringToDuration & StringToBigDecimal
                  // ConvertingCodecs
                  : fieldValue;
            })
        .orElseThrow(
            () ->
                new UnsupportedOperationException("cannot handle logical type for " + fieldValue));
  }

  public static Object handleCassandraCDCLogicalType(Object fieldValue, Schema schema) {
    if (!decodeCDCDataTypes) {
      throw new IllegalStateException(
          "cannot handle CDC logical types because the decodeCDCDataTypes config is false");
    }
    if (fieldValue == null) {
      return null; // logical types are optional
    }
    return getLogicalType(schema)
        .map(
            logicalType -> {
              if (isVarint(logicalType)) {
                return logicalTypeConverters
                    .get(CQL_VARINT)
                    .fromBytes((ByteBuffer) fieldValue, null, null)
                    .toString();
              } else if (isDate(logicalType)) {
                return logicalTypeConverters
                    .get(DATE)
                    .fromInt((int) fieldValue, null, null)
                    .toString();
              } else if (isTimeMicros(logicalType)) {
                return logicalTypeConverters
                    .get(TIME_MICROS)
                    .fromLong((long) fieldValue, null, null)
                    .toString();
              } else if (isTimestampMillis(logicalType)) {
                return logicalTypeConverters
                    .get(TIMESTAMP_MILLIS)
                    .fromLong((long) fieldValue, null, null)
                    .toString();
              }
              return fieldValue instanceof org.apache.avro.generic.GenericRecord
                      && logicalTypeConverters.containsKey(logicalType.getName())
                  ? logicalTypeConverters
                      .get(logicalType.getName())
                      .fromRecord((IndexedRecord) fieldValue, null, null)
                      .toString() // this will utilize the StringToDuration & StringToBigDecimal
                  // ConvertingCodecs
                  : fieldValue;
            })
        .orElseThrow(
            () ->
                new UnsupportedOperationException("cannot handle logical type for " + fieldValue));
  }

  private static boolean isMapOrList(Object mapOrList) {
    return mapOrList instanceof Map || mapOrList instanceof List;
  }

  private static boolean isVarint(LogicalType varint) {
    return CQL_VARINT.equals(varint.getName());
  }

  private static boolean isDate(LogicalType type) {
    return DATE.equals(type.getName());
  }

  private static boolean isTimestampMillis(LogicalType type) {
    return TIMESTAMP_MILLIS.equals(type.getName());
  }

  private static boolean isTimeMicros(LogicalType type) {
    return TIME_MICROS.equals(type.getName());
  }

  private static Optional<LogicalType> getLogicalType(GenericRecord record, String fieldName) {
    if (record.getNativeObject() instanceof org.apache.avro.generic.GenericRecord) {
      org.apache.avro.generic.GenericRecord parentRecord =
          (org.apache.avro.generic.GenericRecord) record.getNativeObject();
      return parentRecord.hasField(fieldName)
          ? getLogicalType(parentRecord.getSchema().getField(fieldName).schema())
          : Optional.empty();
    }
    return Optional.empty();
  }

  private static Optional<LogicalType> getLogicalType(Schema schema) {
    if (schema.isUnion()) {
      return schema
          .getTypes()
          .stream()
          .filter(subSchema -> subSchema.getLogicalType() != null)
          .findFirst()
          .map(subSchema -> subSchema.getLogicalType());
    }

    return Optional.empty();
  }

  public static void enableDecodeCDCDataTypes(boolean decodeCDCDataTypes) {
    if (decodeCDCDataTypes) {
      // Register custom logical types so Avro schema API enables the getLogicalType()
      LogicalTypes.register(CQL_DECIMAL, schema -> new CqlLogicalTypes.CqlDecimalLogicalType());
      LogicalTypes.register(CQL_DURATION, schema -> new CqlLogicalTypes.CqlDurationLogicalType());
      LogicalTypes.register(CQL_VARINT, schema -> new CqlLogicalTypes.CqlVarintLogicalType());

      // Register logical type converters
      logicalTypeConverters.put(CQL_DECIMAL, new CqlLogicalTypes.CqlDecimalConversion());
      logicalTypeConverters.put(CQL_DURATION, new CqlLogicalTypes.CqlDurationConversion());
      logicalTypeConverters.put(CQL_VARINT, new CqlLogicalTypes.CqlVarintConversion());
      logicalTypeConverters.put(DATE, new CqlLogicalTypes.DateConversion());
      logicalTypeConverters.put(TIME_MICROS, new CqlLogicalTypes.TimeConversion());
      logicalTypeConverters.put(TIMESTAMP_MILLIS, new CqlLogicalTypes.TimestampConversion());

      System.out.println("Registered logical types" + logicalTypeConverters.keySet());
    } else {
      // Deregister logical type converters
      logicalTypeConverters.remove(CQL_DECIMAL);
      logicalTypeConverters.remove(CQL_DURATION);
      logicalTypeConverters.remove(CQL_VARINT);
      logicalTypeConverters.remove(DATE);
      logicalTypeConverters.remove(TIME_MICROS);
      logicalTypeConverters.remove(TIMESTAMP_MILLIS);
    }

    AvroTypeUtil.decodeCDCDataTypes = decodeCDCDataTypes;
  }
}
