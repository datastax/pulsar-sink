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
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig;
import java.io.File;
import java.nio.file.Files;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;

@CCMConfig(ssl = true)
public class CassandraTLSTest extends PulsarCCMTestBase {

  public CassandraTLSTest(CCMCluster ccm, @SessionConfig(ssl = true) CqlSession session)
      throws Exception {
    super(ccm, session);
    connectorProperties.put("ssl.provider", "JDK");
    connectorProperties.put(
        "ssl.keystore.path", encodeFile(CcmBridge.DEFAULT_CLIENT_KEYSTORE_FILE));
    connectorProperties.put("ssl.keystore.password", CcmBridge.DEFAULT_CLIENT_KEYSTORE_PASSWORD);
    connectorProperties.put("ssl.hostnameValidation", "false");
    connectorProperties.put(
        "ssl.truststore.path", encodeFile(CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE));
    connectorProperties.put(
        "ssl.truststore.password", CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);
  }

  @Override
  protected void performTest(final PulsarSinkTester pulsarSink) throws PulsarClientException {
    // please note that we are setting the Schema AFTER the creation of the Sink
    try (Producer<MyBean> producer =
        pulsarSink
            .getPulsarClient()
            .newProducer(Schema.AVRO(MyBean.class))
            .topic(pulsarSink.getTopic())
            .create()) {
      producer.newMessage().key("838").value(new MyBean("value1")).send();
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
      }
      assertEquals(1, results.size());
    } finally {
      // always print Sink logs
      pulsarSink.dumpLogs();
    }
  }

  private static String encodeFile(File file) throws Exception {
    byte[] content = Files.readAllBytes(file.toPath());
    return "base64:" + Base64.getEncoder().encodeToString(content);
  }
}
