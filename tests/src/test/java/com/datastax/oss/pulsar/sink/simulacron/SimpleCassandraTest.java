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

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptingExtension;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronExtension;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Column;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Table;
import com.datastax.oss.dsbulk.tests.simulacron.annotations.SimulacronConfig;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;

@SuppressWarnings({"SameParameterValue", "deprecation"})
@ExtendWith(SimulacronExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SimulacronConfig(dseVersion = "5.0.8")
class SimpleCassandraTest {

  private static final Logger log = LoggerFactory.getLogger(SimpleCassandraTest.class);
  private static final String INSTANCE_NAME = "myinstance";
  private final BoundCluster simulacron;
  private final SimulacronUtils.Keyspace schema;
  private final List<Map<String, Object>> taskConfigs = new ArrayList<>();
  private final LogInterceptor logs;
  private final Map<String, Object> connectorProperties;
  private final String hostname;
  private final String port;

  @SuppressWarnings("unused")
  SimpleCassandraTest(BoundCluster simulacron, @LogCapture LogInterceptor logs) throws Exception {
    this.simulacron = simulacron;
    this.logs = logs;

    InetSocketAddress node = simulacron.dc(0).node(0).inetSocketAddress();
    // https://www.testcontainers.org/features/networking/
    hostname = "host.testcontainers.internal";
    port = Integer.toString(node.getPort());
    Testcontainers.exposeHostPorts(node.getPort());

    schema =
        new SimulacronUtils.Keyspace(
            "ks1",
            new Table("table1", new Column("a", DataTypes.INT), new Column("b", DataTypes.TEXT)));

    connectorProperties =
        ImmutableMap.<String, Object>builder()
            .put("name", INSTANCE_NAME)
            .put("contactPoints", hostname)
            .put("port", port)
            .put("loadBalancing.localDc", "dc1")
            .put("topic.mytopic.ks1.table1.mapping", "a=key, b=value.field1")
            .build();
  }

  @Test
  public void test() throws Exception {
    simulacron.clearPrimes(true);
    simulacron.node(0).acceptConnections();
    log.info("start");
    try (PulsarSinkTester pulsarSink = new PulsarSinkTester()) {
      taskConfigs.clear();
      SimulacronUtils.primeTables(simulacron, schema);
      pulsarSink.start();
      log.info("PULSAR deploy sink");
      pulsarSink.deploySink(connectorProperties);
      log.info("finished");
      Thread.sleep(5000);
      pulsarSink.dumpLogs();
    }
  }
}
