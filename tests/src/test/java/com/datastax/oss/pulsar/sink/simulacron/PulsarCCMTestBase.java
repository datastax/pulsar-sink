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
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.CCMExtension;
import com.datastax.oss.dsbulk.tests.driver.VersionUtils;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  protected final String keyspaceName;
  protected final boolean hasDurationType;

  private static final String DEFAULT_MAPPING =
      "a=key, b=value.field1, d=value.mapField, e=value.listField, f=value.pojoUdt, g=value.mapUdt, h=value.setField, "
          + "i=value.listOfMaps, j=value.setOfMaps, k=value.mapOfLists, l=value.mapOfSets, m=value.setOfLists, n=value.listOfSets, s=value.listOfUdt";

  @SuppressWarnings("unused")
  PulsarCCMTestBase(CCMCluster ccm, CqlSession session) throws Exception {
    this(ccm, session, DEFAULT_MAPPING);
  }

  PulsarCCMTestBase(CCMCluster ccm, CqlSession session, String mapping) throws Exception {
    this.session = session;
    int port = ccm.getBinaryPort();
    // https://www.testcontainers.org/features/networking/
    String hostname = "host.testcontainers.internal";
    Testcontainers.exposeHostPorts(port);

    keyspaceName = session.getKeyspace().orElse(CqlIdentifier.fromInternal("unknown")).asInternal();
    // Duration is not supported in Cassandra 3.0
    this.hasDurationType =
        (ccm.getClusterType() == CCMCluster.Type.DSE
            || VersionUtils.isWithinRange(ccm.getCassandraVersion(), Version.parse("4.0.0"), null));
    session.execute(
        SimpleStatement.builder(
                "CREATE TYPE udt (intf int, stringf text, listf frozen<list<text>>, setf frozen<set<int>>, mapf frozen<map<text, double>>)")
            .setTimeout(Duration.ofSeconds(10))
            .build());
    // create UDT with logical types
    session.execute(
        SimpleStatement.builder(
                "CREATE TYPE udtLogicalTypes (decimalf decimal"
                    + (this.hasDurationType ? ", durationf duration" : "")
                    + ", uuidf uuid, varintf varint)")
            .setTimeout(Duration.ofSeconds(10))
            .build());
    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE IF NOT EXISTS table1 ("
                    + "a int PRIMARY KEY, "
                    + "b varchar, "
                    + "c TIMESTAMP, "
                    + "d map<text,text>, "
                    + "e list<text>, "
                    + "f frozen<udt>, "
                    + "g frozen<udt>, " // Non-frozen User-Defined types are not supported in
                    // Cassandra 3.0
                    + "h set<text>, "
                    + "i list<frozen<map<text,text>>>, "
                    + "j set<frozen<map<text,text>>>, "
                    + "k map<text,frozen<list<text>>>, "
                    + "l map<text,frozen<set<text>>>, "
                    + "m set<frozen<list<text>>>, "
                    + "n list<frozen<set<text>>>, "
                    + "o decimal, "
                    + (this.hasDurationType ? "p duration, " : "")
                    + "q uuid, "
                    + "r varint,"
                    + "s list<frozen<udt>>,"
                    + "t frozen<udtLogicalTypes>,"
                    + "u list<frozen<udtLogicalTypes>>)")
            .build());

    connectorProperties = new HashMap<>();
    connectorProperties.put("name", INSTANCE_NAME);
    connectorProperties.put("verbose", "true");
    connectorProperties.put("contactPoints", hostname);
    connectorProperties.put("port", ccm.getBinaryPort());
    connectorProperties.put("batchSize", "1");
    connectorProperties.put("loadBalancing.localDc", ccm.getDC(1));
    connectorProperties.put("topic.mytopic." + keyspaceName + ".table1.mapping", mapping);
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

  public static final class MyKey {

    private int fieldKey;

    public MyKey(int fieldKey) {
      this.fieldKey = fieldKey;
    }

    public int getFieldKey() {
      return fieldKey;
    }

    public void setFieldKey(int fieldKey) {
      this.fieldKey = fieldKey;
    }
  }

  public static final class MyUdt {
    public int intf;
    private String stringf;
    private List<String> listf;
    private Set<Integer> setf;
    private Map<String, Double> mapf;

    public MyUdt(
        int intf, String stringf, List<String> listf, Set<Integer> setf, Map<String, Double> mapf) {
      this.intf = intf;
      this.stringf = stringf;
      this.listf = listf;
      this.setf = setf;
      this.mapf = mapf;
    }

    public int getIntf() {
      return intf;
    }

    public void setIntf(int intf) {
      this.intf = intf;
    }

    public String getStringf() {
      return stringf;
    }

    public void setStringf(String stringf) {
      this.stringf = stringf;
    }

    public List<String> getListf() {
      return listf;
    }

    public void setListf(List<String> listf) {
      this.listf = listf;
    }

    public Set<Integer> getSetf() {
      return setf;
    }

    public void setSetf(Set<Integer> setf) {
      this.setf = setf;
    }

    public Map<String, Double> getMapf() {
      return mapf;
    }

    public void setMapf(Map<String, Double> mapf) {
      this.mapf = mapf;
    }
  }

  public static final class MyBean {

    private String field1;
    private Long longField;
    private Map<String, String> mapField;
    private List<String> listField;
    private Set<String> setField;
    private List<Map<String, String>> listOfMaps;
    private Set<Map<String, String>> setOfMaps;
    private Map<String, List<String>> mapOfLists;
    private Map<String, Set<String>> mapOfSets;
    private List<Set<String>> listOfSets;
    private Set<List<String>> setOfLists;
    private MyUdt pojoUdt;
    private Map<String, Object> mapUdt;
    private Map<String, String> mapUdtFixedType;
    private List<MyUdt> listOfUdt;

    public MyBean(String field1) {
      this.field1 = field1;
    }

    public MyBean(String field1, Long longField) {
      this.field1 = field1;
      this.longField = longField;
    }

    public MyBean(
        String stringField,
        Map<String, String> mapField,
        List<String> listField,
        Set<String> setField,
        List<Map<String, String>> listOfMaps,
        Set<Map<String, String>> setOfMaps,
        Map<String, List<String>> mapOfLists,
        Map<String, Set<String>> mapOfSets,
        List<Set<String>> listOfSets,
        Set<List<String>> setOfLists,
        MyUdt pojoUdt,
        Map<String, Object> mapUdt,
        Map<String, String> mapUdtFixedType,
        List<MyUdt> listOfUdt) {
      this(stringField, Instant.now().toEpochMilli());
      this.mapField = mapField;
      this.listField = listField;
      this.setField = setField;
      this.listOfMaps = listOfMaps;
      this.setOfMaps = setOfMaps;
      this.mapOfLists = mapOfLists;
      this.mapOfSets = mapOfSets;
      this.setOfLists = setOfLists;
      this.listOfSets = listOfSets;
      this.pojoUdt = pojoUdt;
      this.mapUdt = mapUdt;
      this.mapUdtFixedType = mapUdtFixedType;
      this.listOfUdt = listOfUdt;
    }

    public String getField1() {
      return field1;
    }

    public void setField1(String field1) {
      this.field1 = field1;
    }

    public Map<String, String> getMapField() {
      return mapField;
    }

    public void setMapField(Map<String, String> mapField) {
      this.mapField = mapField;
    }

    public List<String> getListField() {
      return listField;
    }

    public void setListField(List<String> listField) {
      this.listField = listField;
    }

    public Set<String> getSetField() {
      return setField;
    }

    public void setSetField(Set<String> setField) {
      this.setField = setField;
    }

    public List<Map<String, String>> getListOfMaps() {
      return listOfMaps;
    }

    public void setListOfMaps(List<Map<String, String>> listOfMaps) {
      this.listOfMaps = listOfMaps;
    }

    public Set<Map<String, String>> getSetOfMaps() {
      return setOfMaps;
    }

    public void setSetOfMaps(Set<Map<String, String>> setOfMaps) {
      this.setOfMaps = setOfMaps;
    }

    public Map<String, List<String>> getMapOfLists() {
      return mapOfLists;
    }

    public void setMapOfLists(Map<String, List<String>> mapOfLists) {
      this.mapOfLists = mapOfLists;
    }

    public Map<String, Set<String>> getMapOfSets() {
      return mapOfSets;
    }

    public void setMapOfSets(Map<String, Set<String>> mapOfSets) {
      this.mapOfSets = mapOfSets;
    }

    public List<Set<String>> getListOfSets() {
      return listOfSets;
    }

    public void setListOfSets(List<Set<String>> listOfSets) {
      this.listOfSets = listOfSets;
    }

    public Set<List<String>> getSetOfLists() {
      return setOfLists;
    }

    public void setSetOfLists(Set<List<String>> setOfLists) {
      this.setOfLists = setOfLists;
    }

    public Long getLongField() {
      return longField;
    }

    public void setLongField(Long longField) {
      this.longField = longField;
    }

    public MyUdt getPojoUdt() {
      return pojoUdt;
    }

    public void setPojoUdt(MyUdt pojoUdt) {
      this.pojoUdt = pojoUdt;
    }

    public Map<String, Object> getMapUdt() {
      return mapUdt;
    }

    public void setMapUdt(Map<String, Object> mapUdt) {
      this.mapUdt = mapUdt;
    }

    public Map<String, String> getMapUdtFixedType() {
      return mapUdtFixedType;
    }

    public void setMapUdtFixedType(Map<String, String> mapUdtFixedType) {
      this.mapUdtFixedType = mapUdtFixedType;
    }

    public List<MyUdt> getListOfUdt() {
      return listOfUdt;
    }

    public void setListOfUdt(List<MyUdt> listOfUdt) {
      this.listOfUdt = listOfUdt;
    }
  }
}
