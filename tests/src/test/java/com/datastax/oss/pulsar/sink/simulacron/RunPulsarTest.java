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

import java.util.Arrays;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** @author enrico.olivelli */
public class RunPulsarTest {

  private static final PulsarContainer pulsarContainer =
      new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar").withTag("2.6.2"))
          .withCommand("bin/pulsar", "standalone") // default one disables worker
          // not a log, as it keeps output compact without additional config - good for debug,
          // not really good for real tests
          .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
          // default waiter doesn't work for function worker being available,
          // leading to 500 on upload otherwise
          .waitingFor(Wait.forLogMessage(".*Function worker service started.*", 1));

  private static PulsarAdmin pulsarAdmin;
  private static PulsarClient pulsarClient;
  private static final String tenant = "public";
  private static final String namespace = "default";
  private static final String sinkname = "mysink";

  @BeforeAll
  static void beforeAll() throws PulsarClientException, PulsarAdminException {
    pulsarContainer.setPortBindings(
        Arrays.asList(
            "8080:" + PulsarContainer.BROKER_HTTP_PORT, "6650:" + PulsarContainer.BROKER_PORT));
    pulsarContainer.start();
    pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(pulsarContainer.getHttpServiceUrl()).build();
    pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
  }

  @Test
  public void testStartContainer() throws Exception {
    System.out.println("creating topic:");
    pulsarAdmin.topics().createNonPartitionedTopic(TOPIC);
    TopicStats stats = pulsarAdmin.topics().getStats(TOPIC);
    System.out.println("stats: " + stats);
  }

  private static final String TOPIC = "persistent://public/default/mytopic";

  @AfterAll
  static void afterAll() throws PulsarClientException, PulsarAdminException {
    pulsarClient.close();
    pulsarAdmin.close();
    pulsarContainer.close();
  }
}
