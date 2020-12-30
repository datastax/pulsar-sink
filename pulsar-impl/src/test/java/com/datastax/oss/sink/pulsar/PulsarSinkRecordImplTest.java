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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;

/** Tests for PulsarSinkRecordImpl */
public class PulsarSinkRecordImplTest {

  @Test
  public void testShortTopic() {
    testTopicName("persistent://public/defaul/mytopic", "mytopic");
    testTopicName("persistent://public/defaul/mytopic-partition-1", "mytopic");
    // edge conditions, that cannot happen in real life
    testTopicName("", "");
    testTopicName("/", "");
    testTopicName("persistent://public/defaul/mytopic-partition-", "mytopic");
  }

  private static void testTopicName(String topic, String expResult) {
    Record record = new PulsarRecordImpl(topic, "key", "value", Schema.STRING);
    String result = PulsarSinkRecordImpl.shortTopic(record);
    assertEquals(expResult, result);
  }
}
