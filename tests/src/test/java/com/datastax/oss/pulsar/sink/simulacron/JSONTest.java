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
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.data.DefaultUdtValue;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;

/** Use JSON - schema is set on the topic */
public class JSONTest extends PulsarCCMTestBase {

  private final Map<String, String> map = ImmutableMap.of("k1", "v1", "k2", "v2");
  private final List<String> list = ImmutableList.of("l1", "l2");
  private final List<Map<String, String>> listOfMaps = ImmutableList.of(map, map);
  private final Set<String> set = ImmutableSet.of("s1", "s2");
  private final Set<Map<String, String>> setOfMaps = ImmutableSet.of(map, map);
  private final Map<String, List<String>> mapOfLists = ImmutableMap.of("k1", list, "k2", list);
  private final Map<String, Set<String>> mapOfSets = ImmutableMap.of("k1", set, "k2", set);
  private final Set<List<String>> setOfLists = ImmutableSet.of(list);
  private final List<Set<String>> listOfSets = ImmutableList.of(set, set);
  private final MyUdt pojoUdt =
      new MyUdt(
          99,
          "random",
          ImmutableList.of("l1", "l2"),
          ImmutableSet.of(3, 4),
          ImmutableMap.of("k1", 7.0D, "k2", 9.0D));
  private final List<MyUdt> listOfUdt = ImmutableList.of(pojoUdt, pojoUdt);
  private final Map<String, Object> mapUdt =
      ImmutableMap.of(
          "intf",
          36,
          "stringf",
          "udt text",
          "listf",
          ImmutableList.of("l1", "l2"),
          "setf",
          ImmutableSet.of(3, 4),
          "mapf",
          ImmutableMap.of("k1", 7.0D, "k2", 9.0D));

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

      producer
          .newMessage()
          .key("838")
          .value(
              new MyBean(
                  "value1",
                  map,
                  list,
                  set,
                  listOfMaps,
                  setOfMaps,
                  mapOfLists,
                  mapOfSets,
                  listOfSets,
                  setOfLists,
                  pojoUdt,
                  mapUdt,
                  null,
                  listOfUdt))
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
        assertEquals(map, row.getMap("d", String.class, String.class));
        assertEquals(list, row.getList("e", String.class));
        assertEquals(set, row.getSet("h", String.class));

        GenericType<List<Map<String, String>>> listOfMapsType =
            new GenericType<List<Map<String, String>>>() {};
        List<Map<String, String>> mapsList = row.get("i", listOfMapsType);
        assertEquals(listOfMaps, mapsList);
        GenericType<Set<Map<String, String>>> setOfMapsType =
            new GenericType<Set<Map<String, String>>>() {};
        Set<Map<String, String>> mapsSet = row.get("j", setOfMapsType);
        assertEquals(setOfMaps, mapsSet);

        GenericType<Map<String, List<String>>> mapOfListsType =
            new GenericType<Map<String, List<String>>>() {};
        Map<String, List<String>> listsMap = row.get("k", mapOfListsType);
        assertEquals(mapOfLists, listsMap);

        GenericType<Map<String, Set<String>>> mapOfSetsType =
            new GenericType<Map<String, Set<String>>>() {};
        Map<String, Set<String>> setsMap = row.get("l", mapOfSetsType);
        assertEquals(mapOfSets, setsMap);

        GenericType<Set<List<String>>> setOfListsType = new GenericType<Set<List<String>>>() {};
        Set<List<String>> listsSet = row.get("m", setOfListsType);
        assertEquals(setOfLists, listsSet);
        GenericType<List<Set<String>>> listOfSetsType = new GenericType<List<Set<String>>>() {};
        List<Set<String>> setsList = row.get("n", listOfSetsType);
        assertEquals(listOfSets, setsList);

        DefaultUdtValue value = (DefaultUdtValue) row.getUdtValue("f");
        assertEquals(value.size(), 5);
        assertEquals(pojoUdt.getIntf(), value.getInt("intf"));
        assertEquals(pojoUdt.getStringf(), value.getString("stringf"));

        GenericType<List<String>> udtListType = new GenericType<List<String>>() {};
        assertEquals(pojoUdt.getListf(), value.get("listf", udtListType));
        GenericType<Set<Integer>> udtSetType = new GenericType<Set<Integer>>() {};
        assertEquals(pojoUdt.getSetf(), value.get("setf", udtSetType));
        GenericType<Map<String, Double>> udtMapType = new GenericType<Map<String, Double>>() {};
        assertEquals(pojoUdt.getMapf(), value.get("mapf", udtMapType));

        value = (DefaultUdtValue) row.getUdtValue("g");
        assertEquals(mapUdt.get("intf"), value.getInt("intf"));
        assertEquals(mapUdt.get("stringf"), value.getString("stringf"));
        assertEquals(mapUdt.get("listf"), value.get("listf", udtListType));
        assertEquals(mapUdt.get("setf"), value.get("setf", udtSetType));
        assertEquals(mapUdt.get("mapf"), value.get("mapf", udtMapType));

        GenericType<List<DefaultUdtValue>> listOfUdtType =
            new GenericType<List<DefaultUdtValue>>() {};
        List<DefaultUdtValue> listOfUdt = row.get("s", listOfUdtType);
        assertEquals(listOfUdt.size(), 2);
        for (UdtValue udt : listOfUdt) {
          assertEquals(pojoUdt.getIntf(), udt.getInt("intf"));
          assertEquals(pojoUdt.getStringf(), udt.getString("stringf"));
          assertEquals(pojoUdt.getListf(), udt.get("listf", udtListType));
          assertEquals(pojoUdt.getSetf(), udt.get("setf", udtSetType));
          assertEquals(pojoUdt.getMapf(), udt.get("mapf", udtMapType));
        }
      }
      assertEquals(1, results.size());
    } finally {
      // always print Sink logs
      pulsarSink.dumpLogs();
    }
  }
}
