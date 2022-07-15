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
import com.datastax.oss.driver.internal.core.data.DefaultUdtValue;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;

/** Use JSON from a String schema topic */
public class JSONFromStringWithLegacyStringTaskTest extends PulsarCCMTestBase {

  public JSONFromStringWithLegacyStringTaskTest(CCMCluster ccm, CqlSession session)
      throws Exception {
    super(ccm, session);
  }

  @Override
  protected void preparePulsarSinkTester(PulsarSinkTester pulsarSink) {
    pulsarSink.setSinkClassName("com.datastax.oss.sink.pulsar.StringCassandraSinkTask");
    pulsarSink.setValueTypeClassName(byte[].class.getName());
  }

  @Override
  protected void performTest(final PulsarSinkTester pulsarSink) throws PulsarClientException {
    // please note that we are setting the Schema AFTER the creation of the Sink
    try (Producer<String> producer =
        pulsarSink
            .getPulsarClient()
            .newProducer(Schema.STRING)
            .topic(pulsarSink.getTopic())
            .create()) {
      producer
          .newMessage()
          .key("838")
          .value("{\"field1\":\"value1\",\"mapField\":{\"k1\":\"v1\",\"k2\":\"v2\"},\"listField\":[\"l1\",\"l2\"],\"udtField\":{\"intf\":99,\"stringf\":\"random\"}}")
          .send();
    }
    try {
      Awaitility.waitAtMost(1, TimeUnit.MINUTES)
          .pollDelay(1, TimeUnit.SECONDS)
          .until(
              () -> {
                List<Row> results = session.execute("SELECT * FROM table1").all();
                return results.size() > 0;
              });

      List<Row> results = session.execute("SELECT * FROM table1").all();
      for (Row row : results) {
        log.info("ROW: " + row);
        assertEquals(838, row.getInt("a"));
        assertEquals("value1", row.getString("b"));
        assertEquals(ImmutableMap.of("k1", "v1", "k2", "v2"), row.getMap("d", String.class, String.class));
        assertEquals(ImmutableList.of("l1", "l2"), row.getList("e", String.class));
        DefaultUdtValue value = (DefaultUdtValue) row.getUdtValue("f");
        assertEquals(value.size(), 2);
        assertEquals(99, value.getInt("f1"));
        assertEquals("random", value.getString("f2"));
      }
      assertEquals(1, results.size());
    } finally {
      // always print Sink logs
      pulsarSink.dumpLogs();
    }
  }
}
