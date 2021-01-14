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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.CCMExtension;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;

@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class PulsarCCMTestBase {

  protected static final Logger log = LoggerFactory.getLogger(PulsarCCMTestBase.class);
  private static final String INSTANCE_NAME = "myinstance";
  private final List<Map<String, Object>> taskConfigs = new ArrayList<>();
  protected final Map<String, Object> connectorProperties;
  protected final CqlSession session;
  private final String keyspaceName;

  @SuppressWarnings("unused")
  PulsarCCMTestBase(CCMCluster ccm, CqlSession session) throws Exception {
    this.session = session;
    int port = ccm.getBinaryPort();
    // https://www.testcontainers.org/features/networking/
    String hostname = "host.testcontainers.internal";
    Testcontainers.exposeHostPorts(port);

    keyspaceName = session.getKeyspace().orElse(CqlIdentifier.fromInternal("unknown")).asInternal();

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE IF NOT EXISTS table1 (" + "a int PRIMARY KEY, " + "b varchar)")
            .setTimeout(Duration.ofSeconds(10))
            .build());

    connectorProperties = new HashMap<>();
    connectorProperties.put("name", INSTANCE_NAME);
    connectorProperties.put("verbose", "true");
    connectorProperties.put("contactPoints", hostname);
    connectorProperties.put("port", ccm.getBinaryPort());
    connectorProperties.put("batchSize", "1");
    connectorProperties.put("loadBalancing.localDc", ccm.getDC(1));
    connectorProperties.put(
        "topic.mytopic." + keyspaceName + ".table1.mapping", "a=key, b=value.field1");
  }

  @Test
  public void test() throws Exception {
    log.info("start");
    try (PulsarSinkTester pulsarSink = new PulsarSinkTester()) {
      taskConfigs.clear();
      preparePulsarSinkTester(pulsarSink);
      pulsarSink.start();
      log.info("PULSAR deploy sink");
      pulsarSink.deploySink(connectorProperties);
      log.info("finished");
      Thread.sleep(5000);
      pulsarSink.dumpLogs();

      performTest(pulsarSink);
    }
  }

  protected abstract void performTest(final PulsarSinkTester pulsarSink) throws Exception;

  protected void preparePulsarSinkTester(PulsarSinkTester pulsarSink) {}

  public static final class MyBean {

    private String field1;

    public MyBean(String field1) {
      this.field1 = field1;
    }

    public String getField1() {
      return field1;
    }

    public void setField1(String field1) {
      this.field1 = field1;
    }
  }
}
