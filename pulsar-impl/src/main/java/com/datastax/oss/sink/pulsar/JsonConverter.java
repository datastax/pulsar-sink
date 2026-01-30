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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

/** Convert an AVRO GenericRecord to a JsonNode. */
public class JsonConverter {

  private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);

  public static JsonNode toJson(GenericRecord genericRecord) {
    if (genericRecord == null) {
      return null;
    }
    ObjectNode objectNode = jsonNodeFactory.objectNode();
    for (Schema.Field field : genericRecord.getSchema().getFields()) {
      objectNode.set(field.name(), toJson(field.schema(), genericRecord.get(field.name())));
    }
    return objectNode;
  }

  public static JsonNode toJson(Schema schema, Object value) {
    if (value == null) {
      return jsonNodeFactory.nullNode();
    }

    if (AvroTypeUtil.shouldHandleCassandraCDCLogicalType(schema)) {
      value = AvroTypeUtil.handleCassandraCDCLogicalType(value, schema);
      if (value instanceof String) {
        return jsonNodeFactory.textNode((String) value);
      }
    }

    switch (schema.getType()) {
      case INT:
        return jsonNodeFactory.numberNode((Integer) value);
      case LONG:
        return jsonNodeFactory.numberNode((Long) value);
      case DOUBLE:
        return jsonNodeFactory.numberNode((Double) value);
      case FLOAT:
        return jsonNodeFactory.numberNode((Float) value);
      case BOOLEAN:
        return jsonNodeFactory.booleanNode((Boolean) value);
      case BYTES:
        if (value instanceof ByteBuffer) {
          return jsonNodeFactory.binaryNode(((ByteBuffer) value).array());
        }
        return jsonNodeFactory.binaryNode((byte[]) value);
      case FIXED:
        return jsonNodeFactory.binaryNode(((GenericFixed) value).bytes());
      case ENUM: // GenericEnumSymbol
      case STRING:
        return jsonNodeFactory.textNode(
            value.toString()); // can be a String or org.apache.avro.util.Utf8
      case ARRAY:
        {
          Schema elementSchema = schema.getElementType();
          ArrayNode arrayNode = jsonNodeFactory.arrayNode();
          Object[] iterable;
          if (value instanceof GenericData.Array) {
            iterable = ((GenericData.Array) value).toArray();
          } else {
            iterable = (Object[]) value;
          }
          for (Object elem : iterable) {
            JsonNode fieldValue = toJson(elementSchema, elem);
            arrayNode.add(fieldValue);
          }
          return arrayNode;
        }
      case MAP:
        {
          Map<Object, Object> map = (Map<Object, Object>) value;
          ObjectNode objectNode = jsonNodeFactory.objectNode();
          for (Map.Entry<Object, Object> entry : map.entrySet()) {
            JsonNode jsonNode = toJson(schema.getValueType(), entry.getValue());
            // can be a String or org.apache.avro.util.Utf8
            final String entryKey = entry.getKey() == null ? null : entry.getKey().toString();
            objectNode.set(entryKey, jsonNode);
          }
          return objectNode;
        }
      case RECORD:
        org.apache.avro.generic.GenericRecord avroRecord =
            (org.apache.avro.generic.GenericRecord) value;
        // Check if this is a tuple record (follows CDC naming convention: Tuple_<hashcode>)
        // Tuples should not be converted to JSON objects; they need to remain as GenericRecords
        // for proper codec processing
        if (isTupleRecord(avroRecord)) {
          // Convert tuple to array representation for proper codec processing
          return createTupleArrayNode(avroRecord);
        }
        return toJson(avroRecord);
      case UNION:
        for (Schema s : schema.getTypes()) {
          if (s.getType() == Schema.Type.NULL) {
            continue;
          }
          return toJson(s, value);
        }
      default:
        return jsonNodeFactory.nullNode();
    }
  }

  /**
   * Check if an Avro GenericRecord represents a tuple by examining its schema name. Tuples follow
   * the CDC naming convention: Tuple_<hashcode>
   */
  private static boolean isTupleRecord(org.apache.avro.generic.GenericRecord record) {
    if (record == null) {
      return false;
    }
    return record.getSchema().getName().startsWith("Tuple_");
  }

  /**
   * Convert a tuple GenericRecord to an ArrayNode representation. Tuples have positional fields
   * (index_0, index_1, etc.) that should be represented as an array for proper codec processing.
   */
  private static JsonNode createTupleArrayNode(org.apache.avro.generic.GenericRecord record) {
    if (record == null) {
      return jsonNodeFactory.nullNode();
    }

    ArrayNode arrayNode = jsonNodeFactory.arrayNode();

    // Tuple fields are named index_0, index_1, index_2, etc.
    // Extract them in order
    int index = 0;
    while (true) {
      String fieldName = "index_" + index;
      Schema.Field field = record.getSchema().getField(fieldName);
      if (field == null) {
        break; // No more fields
      }
      Object fieldValue = record.get(fieldName);
      JsonNode jsonValue = toJson(field.schema(), fieldValue);
      arrayNode.add(jsonValue);
      index++;
    }

    return arrayNode;
  }
}
