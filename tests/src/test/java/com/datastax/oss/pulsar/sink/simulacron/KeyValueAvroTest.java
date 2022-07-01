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
package com.datastax.oss.pulsar.sink.simulacron;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.awaitility.Awaitility;

/** Use KeyVlue(AVRO,AVRO) like in a CDC data flow */
public class KeyValueAvroTest extends PulsarCCMTestBase {

  private static final String MAPPING = "a=key.fieldKey, b=value.field1, c=value.longField";

  public KeyValueAvroTest(CCMCluster ccm, CqlSession session) throws Exception {
    super(ccm, session, MAPPING);
  }

  @Override
  protected void preparePulsarSinkTester(PulsarSinkTester pulsarSink) {
    super.preparePulsarSinkTester(pulsarSink);
    connectorProperties.put("batchSize", "2");
  }

  @Override
  protected void performTest(final PulsarSinkTester pulsarSink) throws PulsarClientException {
    Long long1 = Instant.now().toEpochMilli();
    Long long2 = Instant.now().toEpochMilli() + 1000L;

    // please note that we are setting the Schema AFTER the creation of the Sink
    try (Producer<KeyValue<MyKey, MyBean>> producer =
        pulsarSink
            .getPulsarClient()
            .newProducer(
                Schema.KeyValue(
                    Schema.AVRO(MyKey.class),
                    Schema.AVRO(MyBean.class),
                    KeyValueEncodingType.SEPARATED))
            .topic(pulsarSink.getTopic())
            .create()) {

      producer
          .newMessage()
          .value(new KeyValue<>(new MyKey(838), new MyBean("value1", long1)))
          .send();
      producer
          .newMessage()
          .value(new KeyValue<>(new MyKey(838), new MyBean("value1", null)))
          .send();
      producer
          .newMessage()
          .value(new KeyValue<>(new MyKey(838), new MyBean("value1", null)))
          .send();
      producer
          .newMessage()
          .value(new KeyValue<>(new MyKey(838), new MyBean("value1", long2)))
          .send();
    }
    try {
      Awaitility.waitAtMost(2, TimeUnit.MINUTES)
          .pollDelay(1, TimeUnit.SECONDS)
          .until(
              () -> {
                List<Row> results = session.execute("SELECT * FROM table1").all();
                return results.size() > 0;
              });

      List<Row> results = session.execute("SELECT * FROM table1").all();
      for (Row row : results) {
        log.info("ROW: " + row);
        System.out.println("get c value: " + row.getObject("c"));
        assertEquals(838, row.getInt("a"));
        assertEquals("value1", row.getString("b"));
        assertEquals(long2, row.get("c", TypeCodecs.TIMESTAMP).toEpochMilli());
      }
      assertEquals(1, results.size());
    } finally {
      // always print Sink logs
      pulsarSink.dumpLogs();
    }
  }
}
