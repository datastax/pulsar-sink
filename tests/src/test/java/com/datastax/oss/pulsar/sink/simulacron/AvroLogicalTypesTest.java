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
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.awaitility.Awaitility;

/** Use AVRO */
public class AvroLogicalTypesTest extends PulsarCCMTestBase {
  private static final String MAPPING =
      "a=key, n=value.decimalField, o=value.durationField, p=value.uuidField, q=value.varintField";

  // DECIMAL
  private static final CqlDecimalLogicalType CQL_DECIMAL_LOGICAL_TYPE = new CqlDecimalLogicalType();
  private static final String CQL_DECIMAL = "cql_decimal";
  private static final String CQL_DECIMAL_BIGINT = "bigint";
  private static final String CQL_DECIMAL_SCALE = "scale";
  private static final org.apache.avro.Schema decimalType =
      CQL_DECIMAL_LOGICAL_TYPE.addToSchema(
          SchemaBuilder.record(CQL_DECIMAL)
              .fields()
              .name(CQL_DECIMAL_BIGINT)
              .type()
              .bytesType()
              .noDefault()
              .name(CQL_DECIMAL_SCALE)
              .type()
              .intType()
              .noDefault()
              .endRecord());

  // DURATION
  public static final String CQL_DURATION = "cql_duration";
  public static final String CQL_DURATION_MONTHS = "months";
  public static final String CQL_DURATION_DAYS = "days";
  public static final String CQL_DURATION_NANOSECONDS = "nanoseconds";
  public static final CqlDurationLogicalType CQL_DURATION_LOGICAL_TYPE =
      new CqlDurationLogicalType();
  public static final org.apache.avro.Schema durationType =
      CQL_DURATION_LOGICAL_TYPE.addToSchema(
          SchemaBuilder.record(CQL_DURATION)
              .fields()
              .name(CQL_DURATION_MONTHS)
              .type()
              .intType()
              .noDefault()
              .name(CQL_DURATION_DAYS)
              .type()
              .intType()
              .noDefault()
              .name(CQL_DURATION_NANOSECONDS)
              .type()
              .longType()
              .noDefault()
              .endRecord());

  // UUID
  private static final org.apache.avro.Schema uuidType =
      LogicalTypes.uuid()
          .addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));

  // VARINT
  private static final String CQL_VARINT = "cql_varint";
  private static final CqlVarintLogicalType CQL_VARINT_LOGICAL_TYPE = new CqlVarintLogicalType();
  private static final org.apache.avro.Schema varintType =
      CQL_VARINT_LOGICAL_TYPE.addToSchema(
          org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES));

  public AvroLogicalTypesTest(CCMCluster ccm, CqlSession session) throws Exception {
    super(ccm, session, MAPPING);
  }

  @Override
  protected void performTest(final PulsarSinkTester pulsarSink) throws PulsarClientException {
    List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
    fields.add(new org.apache.avro.Schema.Field("decimalField", decimalType));
    fields.add(new org.apache.avro.Schema.Field("durationField", durationType));
    fields.add(new org.apache.avro.Schema.Field("uuidField", uuidType));
    fields.add(new org.apache.avro.Schema.Field("varintField", varintType));

    org.apache.avro.generic.GenericRecord decimalRecord = new GenericData.Record(decimalType);
    BigDecimal decimal = new BigDecimal(314.16);
    decimalRecord.put(CQL_DECIMAL_BIGINT, ByteBuffer.wrap(decimal.unscaledValue().toByteArray()));
    decimalRecord.put(CQL_DECIMAL_SCALE, decimal.scale());

    CqlDuration duration = CqlDuration.newInstance(1, 2, 320688000000000L);
    org.apache.avro.generic.GenericRecord durationRecord = new GenericData.Record(durationType);
    durationRecord.put(CQL_DURATION_MONTHS, duration.getMonths());
    durationRecord.put(CQL_DURATION_DAYS, duration.getDays());
    durationRecord.put(CQL_DURATION_NANOSECONDS, duration.getNanoseconds());

    org.apache.avro.Schema avroSchema =
        org.apache.avro.Schema.createRecord("logical_types", "", "ns1", false, fields);
    org.apache.avro.generic.GenericRecord logicalTypesRecord = new GenericData.Record(avroSchema);
    logicalTypesRecord.put("decimalField", decimalRecord);
    logicalTypesRecord.put("durationField", durationRecord);
    UUID uuid = UUID.randomUUID();
    logicalTypesRecord.put("uuidField", uuid.toString());
    BigInteger bigInteger = new BigInteger("314");
    logicalTypesRecord.put("varintField", ByteBuffer.wrap(bigInteger.toByteArray()));
    Schema pulsarSchema = new NativeSchemaWrapper(avroSchema, SchemaType.AVRO);

    try (Producer<byte[]> producer =
        pulsarSink
            .getPulsarClient()
            .newProducer(pulsarSchema)
            .topic(pulsarSink.getTopic())
            .create()) {
      producer
          .newMessage()
          .key("838")
          .value(serializeAvroGenericRecord(logicalTypesRecord, avroSchema))
          .send();
    }
    try {
      Awaitility.waitAtMost(30, TimeUnit.SECONDS)
          .pollDelay(1, TimeUnit.SECONDS)
          .until(
              () -> {
                List<Row> results = session.execute("SELECT a, n, o, p, q FROM table1").all();
                return results.size() > 0;
              });

      List<Row> results = session.execute("SELECT a, n, o, p, q FROM table1").all();
      for (Row row : results) {
        log.info("ROW: " + row);
        assertEquals(838, row.getInt("a"));
        assertEquals(decimal, row.getBigDecimal("n"));
        assertEquals(duration, row.getCqlDuration("o"));
        assertEquals(uuid, row.getUuid("p"));
        assertEquals(bigInteger, row.getBigInteger("q"));
      }
      assertEquals(1, results.size());
    } finally {
      // always print Sink logs
      pulsarSink.dumpLogs();
    }
  }

  private static class CqlDecimalLogicalType extends LogicalType {
    public CqlDecimalLogicalType() {
      super(CQL_DECIMAL);
    }
  }

  private static class CqlDurationLogicalType extends LogicalType {
    public CqlDurationLogicalType() {
      super(CQL_DURATION);
    }
  }

  private static class CqlVarintLogicalType extends LogicalType {
    public CqlVarintLogicalType() {
      super(CQL_VARINT);
    }
  }

  private static class NativeSchemaWrapper implements org.apache.pulsar.client.api.Schema<byte[]> {

    private final SchemaInfo pulsarSchemaInfo;
    private final org.apache.avro.Schema nativeSchema;

    private final SchemaType pulsarSchemaType;

    private final SpecificDatumWriter datumWriter;

    public NativeSchemaWrapper(org.apache.avro.Schema nativeSchema, SchemaType pulsarSchemaType) {
      this.nativeSchema = nativeSchema;
      this.pulsarSchemaType = pulsarSchemaType;
      this.pulsarSchemaInfo =
          SchemaInfoImpl.builder()
              .schema(nativeSchema.toString(false).getBytes(StandardCharsets.UTF_8))
              .properties(new HashMap<>())
              .type(pulsarSchemaType)
              .name(nativeSchema.getName())
              .build();
      this.datumWriter = new SpecificDatumWriter<>(this.nativeSchema);
    }

    @Override
    public byte[] encode(byte[] bytes) {
      return bytes;
    }

    @Override
    public SchemaInfo getSchemaInfo() {
      return pulsarSchemaInfo;
    }

    @Override
    public NativeSchemaWrapper clone() {
      return new NativeSchemaWrapper(nativeSchema, pulsarSchemaType);
    }

    @Override
    public void validate(byte[] message) {
      // nothing to do
    }

    @Override
    public boolean supportSchemaVersioning() {
      return true;
    }

    @Override
    public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {}

    @Override
    public byte[] decode(byte[] bytes) {
      return bytes;
    }

    @Override
    public boolean requireFetchingSchemaInfo() {
      return true;
    }

    @Override
    public void configureSchemaInfo(String topic, String componentName, SchemaInfo schemaInfo) {}

    @Override
    public Optional<Object> getNativeSchema() {
      return Optional.of(nativeSchema);
    }
  }

  public static byte[] serializeAvroGenericRecord(
      org.apache.avro.generic.GenericRecord genericRecord, org.apache.avro.Schema schema) {
    try {
      SpecificDatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
      datumWriter.write(genericRecord, binaryEncoder);
      binaryEncoder.flush();
      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
