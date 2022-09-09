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
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.data.DefaultUdtValue;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;

/** Use JSON from a String schema topic */
public class JSONFromStringWithLegacyStringTaskTest extends PulsarCCMTestBase {

  private static final String MAPPING =
          "a=key, b=value.field1, d=value.mapField, e=value.listField, f=value.udtField, h=value.setField, "
                  + "i=value.listOfMaps, j=value.setOfMaps, k=value.mapOfLists, l=value.mapOfSets, m=value.setOfLists, n=value.listOfSets";

  private static final String JSON = "{\"field1\":\"value1\",\"mapField\":{\"k1\":\"v1\",\"k2\":\"v2\"},\"listField\":[\"l1\",\"l2\"],\"setField\":[\"s1\",\"s2\"],"
          +  "\"listOfMaps\":[{\"k1\":\"v1\",\"k2\":\"v2\"}, {\"k1\":\"v1\",\"k2\":\"v2\"}],"
          +  "\"setOfMaps\":[{\"k1\":\"v1\",\"k2\":\"v2\"}],"
          +  "\"mapOfLists\":{\"k1\":[\"l1\",\"l2\"],\"k2\":[\"l1\",\"l2\"]},"
          +  "\"mapOfSets\":{\"k1\":[\"s1\",\"s2\"],\"k2\":[\"s1\",\"s2\"]},"
          +  "\"setOfLists\":[[\"l1\",\"l2\"]],"
          +  "\"listOfSets\":[[\"s1\",\"s2\"],[\"s1\",\"s2\"]],"
          + "\"udtField\":{\"intf\":99,\"stringf\":\"random\",\"mapf\":{\"k1\":7.0,\"k2\":9.0},\"listf\":[\"l1\",\"l2\"],\"setf\":[3,4]}}";

  public JSONFromStringWithLegacyStringTaskTest(CCMCluster ccm, CqlSession session)
      throws Exception {
    super(ccm, session, MAPPING);
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
          .value(JSON).send();
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
      List list = ImmutableList.of("l1", "l2");
      Map map = ImmutableMap.of("k1", "v1", "k2", "v2");
      Set set = ImmutableSet.of("s1", "s2");
      for (Row row : results) {
        log.info("ROW: " + row);
        assertEquals(838, row.getInt("a"));
        assertEquals("value1", row.getString("b"));
        assertEquals(map, row.getMap("d", String.class, String.class));
        assertEquals(list, row.getList("e", String.class));
        assertEquals(set, row.getSet("h", String.class));

        GenericType<List<Map<String, String>>> listOfMapsType = new GenericType<List<Map<String, String>>>() {};
        List<Map<String, String>> mapsList = row.get("i", listOfMapsType);
        assertEquals(ImmutableList.of(map, map), mapsList);
        GenericType<Set<Map<String, String>>> setOfMapsType = new GenericType<Set<Map<String, String>>>() {};
        Set<Map<String, String>> mapsSet = row.get("j", setOfMapsType);
        assertEquals(ImmutableSet.of(map), mapsSet);

        GenericType<Map<String, List<String>>> mapOfListsType = new GenericType<Map<String, List<String>>>() {};
        Map<String, List<String>> listsMap = row.get("k", mapOfListsType);
        assertEquals(ImmutableMap.of("k1", list, "k2", list), listsMap);

        GenericType<Map<String, Set<String>>> mapOfSetsType = new GenericType<Map<String, Set<String>>>() {};
        Map<String, Set<String>> setsMap = row.get("l", mapOfSetsType);
        assertEquals(ImmutableMap.of("k1", set, "k2", set), setsMap);

        GenericType<Set<List<String>>> setOfListsType = new GenericType<Set<List<String>>>() {};
        Set<List<String>> listsSet = row.get("m", setOfListsType);
        assertEquals(ImmutableSet.of(list), listsSet);
        GenericType<List<Set<String>>> listOfSetsType = new GenericType<List<Set<String>>>() {};
        List<Set<String>> setsList = row.get("n", listOfSetsType);
        assertEquals(ImmutableList.of(set, set), setsList);

        DefaultUdtValue value = (DefaultUdtValue) row.getUdtValue("f");
        assertEquals(value.size(), 5);
        assertEquals(99, value.getInt("intf"));
        assertEquals("random", value.getString("stringf"));

        GenericType<List<String>> udtListType = new GenericType<List<String>>() {};
        assertEquals(ImmutableList.of("l1", "l2"), value.get("listf", udtListType));
        GenericType<Set<Integer>> udtSetType = new GenericType<Set<Integer>>() {};
        assertEquals(ImmutableSet.of(3, 4), value.get("setf", udtSetType));
        GenericType<Map<String, Double>> udtMapType = new GenericType<Map<String, Double>>() {};
        assertEquals(ImmutableMap.of("k1", 7.0D,"k2",9.0D), value.get("mapf", udtMapType));
      }
      assertEquals(1, results.size());
    } finally {
      // always print Sink logs
      pulsarSink.dumpLogs();
    }
  }
}
