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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Utility to start an deploy the sink in a real Pulsar env */
public class PulsarSinkTester implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(PulsarSinkTester.class);

  private static final String IMAGE_NAME =
      System.getProperty("pulsar.image", "apachepulsar/pulsar");
  private static final String IMAGE_VERSION = System.getProperty("pulsar.image.version", "2.6.2");

  //  private static final String IMAGE_NAME = "datastax/pulsar-all";
  //  private static final String IMAGE_VERSION = "latest";
  private PulsarContainer pulsarContainer;
  private PulsarAdmin pulsarAdmin;
  private PulsarClient pulsarClient;
  private final String tenant = "public";
  private final String namespace = "default";
  private final String sinkname = "mysink";

  private final String topic = "persistent://public/default/mytopic";
  private String sinkClassName;
  private String valueTypeClassName;

  public void start() throws Exception {
    log.info("Starting Pulsar Docker Container: " + IMAGE_NAME + ":" + IMAGE_VERSION);
    pulsarContainer =
        new PulsarContainer(
                DockerImageName.parse(IMAGE_NAME)
                    .withTag(IMAGE_VERSION)
                    .asCompatibleSubstituteFor("apachepulsar/pulsar"))
            .withCommand("bin/pulsar", "standalone") // default one disables worker
            // not a log, as it keeps output compact without additional config - good for debug,
            // not really good for real tests
            .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
            // default waiter doesn't work for function worker being available,
            // leading to 500 on upload otherwise
            .waitingFor(Wait.forLogMessage(".*Function worker service started.*", 1));
    pulsarContainer.setPortBindings(
        Arrays.asList(
            "8080:" + PulsarContainer.BROKER_HTTP_PORT, "6650:" + PulsarContainer.BROKER_PORT));
    pulsarContainer.start();
    System.out.println("PULSAR-CONTAINER-ID: " + pulsarContainer.getContainerId());
    pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(pulsarContainer.getHttpServiceUrl()).build();
    pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
  }

  private String grabSinkLogs() {
    String fileName;
    if (IMAGE_VERSION.startsWith("2.6")) {
      fileName =
          "/tmp/functions/" + tenant + "/" + namespace + "/" + sinkname + "/" + sinkname + "-0.log";
    } else {
      fileName =
          "/pulsar/logs/functions/"
              + tenant
              + "/"
              + namespace
              + "/"
              + sinkname
              + "/"
              + sinkname
              + "-0.log";
    }
    log.info("Log file: " + fileName);
    StringBuilder buffer = new StringBuilder();
    pulsarContainer.copyFileFromContainer(
        fileName,
        (is) -> {
          InputStreamReader reader = new InputStreamReader(is, "utf-8");
          int c = reader.read();
          while (c != -1) {
            buffer.append((char) c);
            c = reader.read();
          }
          return null;
        });
    return buffer.toString();
  }

  public void deploySink(Map<String, Object> connectorProperties) throws Exception {
    log.info("creating topic:");
    Thread.sleep(30000);
    pulsarAdmin.topics().createNonPartitionedTopic(topic);
    TopicStats stats = pulsarAdmin.topics().getStats(topic);
    log.info("stats: " + stats);
    SinkConfig sinkConfig =
        SinkConfig.builder()
            .configs(connectorProperties)
            .sourceSubscriptionName("mysink")
            .name(sinkname)
            .namespace(namespace)
            .tenant(tenant)
            .className(sinkClassName != null ? sinkClassName : null)
            .inputs(Collections.singleton(topic))
            .build();
    String narPath = System.getProperty("narFile");
    log.info("create sink, narFile is " + narPath);
    pulsarAdmin.sinks().createSink(sinkConfig, narPath);
    List<String> sinks = pulsarAdmin.sinks().listSinks(tenant, namespace);
    log.info("sinks:" + sinks);
    assertTrue(sinks.contains(sinkname));
    Thread.sleep(5000);
    String logs = grabSinkLogs();
    log.info("LOGS: " + logs);

    if (sinkClassName != null) {
      assertThat(logs, containsString("typeClassName: \"" + this.valueTypeClassName + "\""));
      assertThat(logs, containsString("className: \"" + this.sinkClassName + "\""));

    } else {
      assertThat(
          logs,
          containsString("typeClassName: \"org.apache.pulsar.client.api.schema.GenericRecord\""));
      assertThat(
          logs,
          containsString("className: \"com.datastax.oss.sink.pulsar.RecordCassandraSinkTask\""));
    }
  }

  public void dumpLogs() {
    String logs = grabSinkLogs();
    log.info("LOGS: " + logs);
  }

  public PulsarAdmin getPulsarAdmin() {
    return pulsarAdmin;
  }

  public PulsarClient getPulsarClient() {
    return pulsarClient;
  }

  public String getTopic() {
    return topic;
  }

  public void close() throws PulsarClientException, PulsarAdminException {
    if (pulsarClient != null) {
      pulsarClient.close();
    }
    if (pulsarAdmin != null) {
      pulsarAdmin.close();
    }
    if (pulsarContainer != null) {
      pulsarContainer.close();
    }
  }

  public void setSinkClassName(String sinkClassName) {
    this.sinkClassName = sinkClassName;
  }

  public void setValueTypeClassName(String valueTypeClassName) {
    this.valueTypeClassName = valueTypeClassName;
  }
}
