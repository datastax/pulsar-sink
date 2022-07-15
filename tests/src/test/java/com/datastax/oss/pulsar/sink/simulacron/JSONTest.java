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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;

/** Use JSON - schema is set on the topic */
public class JSONTest extends PulsarCCMTestBase {

  private final Map<String, String> map = ImmutableMap.of("k1", "v1", "k2", "v2");
  private final List<String> list = ImmutableList.of("l1", "l2");

  private final MyUdt udt = new MyUdt(99, "random");

  public JSONTest(CCMCluster ccm, CqlSession session) throws Exception {
    super(ccm, session);
  }

  @Override
  protected void performTest(final PulsarSinkTester pulsarSink) throws PulsarClientException {
    // please note that we are setting the Schema AFTER the creation of the Sink
    try (Producer<MyBean> producer =
        pulsarSink
            .getPulsarClient()
            .newProducer(Schema.JSON(MyBean.class))
            .topic(pulsarSink.getTopic())
            .create()) {

      producer.newMessage().key("838").value(new MyBean("value1", map, list, udt)).send();
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
        assertEquals(map, row.getMap("d", String.class, String.class));
        assertEquals(list, row.getList("e", String.class));
        DefaultUdtValue value = (DefaultUdtValue) row.getUdtValue("f");
        assertEquals(value.size(), 2);
        assertEquals(udt.getIntf(), value.getInt("intf"));
        assertEquals(udt.getStringf(), value.getString("stringf"));
      }
      assertEquals(1, results.size());
    } finally {
      // always print Sink logs
      pulsarSink.dumpLogs();
    }
  }
}
