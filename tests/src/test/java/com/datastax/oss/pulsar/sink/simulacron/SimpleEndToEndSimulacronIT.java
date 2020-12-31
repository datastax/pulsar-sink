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

import static com.datastax.oss.dsbulk.tests.logging.StreamType.STDERR;
import static com.datastax.oss.dsbulk.tests.logging.StreamType.STDOUT;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamCapture;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptor;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@SuppressWarnings({"SameParameterValue", "deprecation"})
@ExtendWith(SimulacronExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SimulacronConfig(dseVersion = "5.0.8")
class SimpleEndToEndSimulacronIT {

  private static final String INSTANCE_NAME = "myinstance";
  private final BoundCluster simulacron;
  private final SimulacronUtils.Keyspace schema;
  private final List<Map<String, Object>> taskConfigs = new ArrayList<>();
  private final LogInterceptor logs;
  private final Map<String, Object> connectorProperties;
  private final String hostname;
  private final String port;

  @SuppressWarnings("unused")
  SimpleEndToEndSimulacronIT(
      BoundCluster simulacron,
      @LogCapture LogInterceptor logs,
      @StreamCapture(STDOUT) StreamInterceptor stdOut,
      @StreamCapture(STDERR) StreamInterceptor stdErr) {

    this.simulacron = simulacron;
    this.logs = logs;

    InetSocketAddress node = simulacron.dc(0).node(0).inetSocketAddress();
    hostname = node.getHostName();
    port = Integer.toString(node.getPort());

    schema =
        new SimulacronUtils.Keyspace(
            "ks1",
            new Table("table1", new Column("a", DataTypes.INT), new Column("b", DataTypes.TEXT)),
            new Table(
                "table1_with_ttl", new Column("a", DataTypes.INT), new Column("b", DataTypes.TEXT)),
            new Table("table2", new Column("a", DataTypes.INT), new Column("b", DataTypes.TEXT)),
            new Table(
                "mycounter",
                new Column("a", DataTypes.INT),
                new Column("b", DataTypes.TEXT),
                new Column("c", DataTypes.COUNTER)),
            new Table(
                "table1_custom_query",
                new Column("col1", DataTypes.INT),
                new Column("col2", DataTypes.TEXT)));

    connectorProperties =
        ImmutableMap.<String, Object>builder()
            .put("name", INSTANCE_NAME)
            .put("contactPoints", hostname)
            .put("port", port)
            .put("loadBalancing.localDc", "dc1")
            .put("topic.mytopic.ks1.table1.mapping", "a=key, b=value.field1")
            .put("topic.mytopic_with_ttl.ks1.table1_with_ttl.mapping", "a=key, b=value, __ttl=key")
            .put("topic.yourtopic.ks1.table2.mapping", "a=key, b=value.field1")
            .build();
  }

  @BeforeEach
  void resetPrimes() {
    simulacron.clearPrimes(true);
    simulacron.node(0).acceptConnections();
    taskConfigs.clear();
  }

  @AfterEach
  void cleanupMetrics() {}

  @AfterEach
  void stopConnector() {}

  @Test
  void test() {
    SimulacronUtils.primeTables(simulacron, schema);
  }
}
