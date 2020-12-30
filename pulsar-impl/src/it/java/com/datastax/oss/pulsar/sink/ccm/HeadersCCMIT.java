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
package com.datastax.oss.pulsar.sink.ccm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.common.sink.ConfigException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.sink.pulsar.PulsarRecordImpl;
import java.util.List;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class HeadersCCMIT extends EndToEndCCMITBase {

  HeadersCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
  }

  @Test
  void should_delete_when_header_values_are_null() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    taskConfigs.add(
        makeConnectorProperties("my_pk=header.my_pk, my_value=header.my_value", "pk_value", null));

    PulsarRecordImpl record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, 1L, Schema.INT64)
            .setProperty("my_pk", 1234567L + "")
            .setProperty("my_value", null);

    runTaskWithRecords(record);

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  /** Test for KAF-142 */
  @Test
  void should_use_values_from_header_in_mapping() {
    // values used in this test are random and irrelevant for the test
    // given
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=header.bigint,"
                + "doublecol=header.double,"
                + "textcol=header.text,"
                + "booleancol=header.boolean,"
                + "floatcol=header.float,"
                + "intcol=header.int"));

    Long baseValue = 1234567L;
    String json = "{\"bigint\": 1234567}";
    PulsarRecordImpl record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", json, 1L, Schema.INT64)
            .setProperty("bigint", baseValue + "")
            .setProperty("double", baseValue.doubleValue() + "")
            .setProperty("text", "value")
            .setProperty("boolean", "false")
            .setProperty("float", baseValue.floatValue() + "")
            .setProperty("int", baseValue.intValue() + "");

    // when
    runTaskWithRecords(record);

    // then
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(baseValue.longValue());
    assertThat(row.getDouble("doublecol")).isEqualTo(baseValue.doubleValue());
    assertThat(row.getString("textcol")).isEqualTo("value");
    assertThat(row.getBoolean("booleancol")).isEqualTo(false);
    assertThat(row.getFloat("floatcol")).isEqualTo(baseValue.floatValue());
    assertThat(row.getInt("intcol")).isEqualTo(baseValue.intValue());
  }

  @Test
  void should_fail_when_using_header_without_specific_field_in_a_mapping() {
    taskConfigs.add(makeConnectorProperties("bigintcol=key, udtcol=header"));

    PulsarRecordImpl record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, 1234L, Schema.INT64)
            .setProperty("myproperty", "value");

    assertThatThrownBy(() -> runTaskWithRecords(record))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            "Invalid field name 'header': field names in mapping must be 'key', 'value', or start with 'key.' or 'value.' or 'header.', or be one of supported functions: '[now()]'");
  }
}
