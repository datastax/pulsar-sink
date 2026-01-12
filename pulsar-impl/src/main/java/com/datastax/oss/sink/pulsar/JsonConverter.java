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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Convert an AVRO GenericRecord to a JsonNode. */
public class JsonConverter {

  private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
  private static final Logger log = LoggerFactory.getLogger(JsonConverter.class);

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
    log.info("----------Converting to json-------");
    log.info("Schema name {}", schema.getName());
    if (AvroTypeUtil.shouldHandleCassandraCDCLogicalType(schema)) {
      value = AvroTypeUtil.handleCassandraCDCLogicalType(value, schema);
      if (value instanceof String) {
        return jsonNodeFactory.textNode((String) value);
      }
    }

    switch (schema.getType()) {
      case INT:
        log.info("---------Coming as int--------");
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
        log.info("--------Coming as record--------");
        return toJson((GenericRecord) value);
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
}
