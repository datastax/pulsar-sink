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

import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AvroTypeUtil {

  private static final Logger log = LoggerFactory.getLogger(AvroTypeUtil.class);

  private AvroTypeUtil() {}

  public static boolean shouldWrapAvroType(GenericRecord record, String fieldName) {
    try {
      return record != null
          && record.getSchemaType() == SchemaType.AVRO
          && isMapOrList(record.getField(fieldName));
    } catch (UnsupportedOperationException ex) {
      log.warn("record {} does not implement getSchemaType", record);
      return false;
    }
  }

  private static boolean isMapOrList(Object mapOrList) {
    return mapOrList instanceof Map || mapOrList instanceof List;
  }
}
