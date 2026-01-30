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
package com.datastax.oss.sink.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.common.sink.metadata.InnerDataAndMetadata;
import com.datastax.oss.common.sink.metadata.MetadataCreator;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;

class MetadataCreatorTest {

  private static final PrimitiveType CQL_TYPE = new PrimitiveType(-1);

  public static class MyPojo {

    String name;
    int age;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }
  }

  public static class MyPojoWithBlob {

    String name;
    int age;
    byte[] blob;
    ByteBuffer blobBuffer;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }

    public byte[] getBlob() {
      return blob;
    }

    public void setBlob(byte[] blob) {
      this.blob = blob;
    }

    public ByteBuffer getBlobBuffer() {
      return blobBuffer;
    }

    public void setBlobBuffer(ByteBuffer blobBuffer) {
      this.blobBuffer = blobBuffer;
    }
  }

  public static class MyKey {

    int pk;

    public int getPk() {
      return pk;
    }

    public void setPk(int pk) {
      this.pk = pk;
    }
  }

  @Test
  void shouldCreateMetadataForStruct() throws IOException {
    // given
    Schema schema = Schema.JSON(MyPojo.class);
    GenericRecord object = new GenericRecordImpl().put("name", "Bobby McGee").put("age", 21);
    Record<GenericRecord> record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, object, schema);

    LocalSchemaRegistry localSchemaRegistry = new LocalSchemaRegistry();
    PulsarSinkRecordImpl pulsarSinkRecordImpl =
        new PulsarSinkRecordImpl(record, localSchemaRegistry);

    InnerDataAndMetadata innerDataAndMetadata =
        MetadataCreator.makeMeta(pulsarSinkRecordImpl.value());

    // then
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("name")).isEqualTo("Bobby McGee");
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("age")).isEqualTo(21);
    assertThat(innerDataAndMetadata.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("name", CQL_TYPE))
        .isEqualTo(GenericType.STRING);
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("age", CQL_TYPE))
        .isEqualTo(GenericType.INTEGER);
  }

  @Test
  void shouldCreateMetadataForKeyValue() throws IOException {
    // given
    Schema keySchema = Schema.AVRO(MyKey.class);
    Schema valueSchema = Schema.AVRO(MyPojo.class);
    Schema schema = Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.SEPARATED);
    GenericRecord keyObject = new GenericRecordImpl().put("pk", 15);
    GenericRecord valueObject = new GenericRecordImpl().put("name", "Bobby McGee").put("age", 21);
    GenericRecord object = new GenericRecordImpl(new KeyValue<>(keyObject, valueObject));
    Record<GenericRecord> record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, object, schema);

    LocalSchemaRegistry localSchemaRegistry = new LocalSchemaRegistry();
    PulsarSinkRecordImpl pulsarSinkRecordImpl =
        new PulsarSinkRecordImpl(record, localSchemaRegistry);

    InnerDataAndMetadata innerDataAndMetadataValue =
        MetadataCreator.makeMeta(pulsarSinkRecordImpl.value());

    InnerDataAndMetadata innerDataAndMetadataKey =
        MetadataCreator.makeMeta(pulsarSinkRecordImpl.key());

    // then

    assertThat(innerDataAndMetadataKey.getInnerData().getFieldValue("pk")).isEqualTo(15);
    assertThat(innerDataAndMetadataKey.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadataKey.getInnerMetadata().getFieldType("pk", CQL_TYPE))
        .isEqualTo(GenericType.INTEGER);

    assertThat(innerDataAndMetadataValue.getInnerData().getFieldValue("name"))
        .isEqualTo("Bobby McGee");
    assertThat(innerDataAndMetadataValue.getInnerData().getFieldValue("age")).isEqualTo(21);
    assertThat(innerDataAndMetadataValue.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadataValue.getInnerMetadata().getFieldType("name", CQL_TYPE))
        .isEqualTo(GenericType.STRING);
    assertThat(innerDataAndMetadataValue.getInnerMetadata().getFieldType("age", CQL_TYPE))
        .isEqualTo(GenericType.INTEGER);
  }

  @Test
  void shouldCreateMetadataForStringTopicJsonEncodedValueJsonEncodedString() throws IOException {
    // given
    Schema schema = Schema.STRING;
    GenericRecord object = new GenericRecordImpl("{\"name\":\"Bobby McGee\",\"age\":21}");
    Record<GenericRecord> record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", "{\"pk\":15}", object, schema);

    LocalSchemaRegistry localSchemaRegistry = new LocalSchemaRegistry();
    PulsarSinkRecordImpl pulsarSinkRecordImpl =
        new PulsarSinkRecordImpl(record, localSchemaRegistry);

    InnerDataAndMetadata innerDataAndMetadataValue =
        MetadataCreator.makeMeta(pulsarSinkRecordImpl.value());

    InnerDataAndMetadata innerDataAndMetadataKey =
        MetadataCreator.makeMeta(pulsarSinkRecordImpl.key());

    // then

    assertThat(innerDataAndMetadataKey.getInnerData().getFieldValue("pk"))
        .isEqualTo(IntNode.valueOf(15));
    assertThat(innerDataAndMetadataKey.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadataKey.getInnerMetadata().getFieldType("pk", CQL_TYPE))
        .isEqualTo(GenericType.of(JsonNode.class));

    assertThat(innerDataAndMetadataValue.getInnerData().getFieldValue("name"))
        .isEqualTo(TextNode.valueOf("Bobby McGee"));
    assertThat(innerDataAndMetadataValue.getInnerData().getFieldValue("age"))
        .isEqualTo(IntNode.valueOf(21));
    assertThat(innerDataAndMetadataValue.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadataValue.getInnerMetadata().getFieldType("name", CQL_TYPE))
        .isEqualTo(GenericType.of(JsonNode.class));
    assertThat(innerDataAndMetadataValue.getInnerMetadata().getFieldType("age", CQL_TYPE))
        .isEqualTo(GenericType.of(JsonNode.class));
  }

  @Test
  void shouldCreateMetadataForStringTopicJsonEncodedValueJsonEncodedStringAsByteArray()
      throws IOException {
    // given
    Schema schema = Schema.BYTES;
    byte[] object = "{\"name\":\"Bobby McGee\",\"age\":21}".getBytes(StandardCharsets.UTF_8);
    Record<GenericRecord> record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", "{\"pk\":15}", object, schema);

    LocalSchemaRegistry localSchemaRegistry = new LocalSchemaRegistry();
    PulsarSinkRecordImpl pulsarSinkRecordImpl =
        new PulsarSinkRecordImpl(record, localSchemaRegistry);

    InnerDataAndMetadata innerDataAndMetadataValue =
        MetadataCreator.makeMeta(pulsarSinkRecordImpl.value());

    InnerDataAndMetadata innerDataAndMetadataKey =
        MetadataCreator.makeMeta(pulsarSinkRecordImpl.key());

    // then

    assertThat(innerDataAndMetadataKey.getInnerData().getFieldValue("pk"))
        .isEqualTo(IntNode.valueOf(15));
    assertThat(innerDataAndMetadataKey.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadataKey.getInnerMetadata().getFieldType("pk", CQL_TYPE))
        .isEqualTo(GenericType.of(JsonNode.class));

    assertThat(innerDataAndMetadataValue.getInnerData().getFieldValue("name"))
        .isEqualTo(TextNode.valueOf("Bobby McGee"));
    assertThat(innerDataAndMetadataValue.getInnerData().getFieldValue("age"))
        .isEqualTo(IntNode.valueOf(21));
    assertThat(innerDataAndMetadataValue.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadataValue.getInnerMetadata().getFieldType("name", CQL_TYPE))
        .isEqualTo(GenericType.of(JsonNode.class));
    assertThat(innerDataAndMetadataValue.getInnerMetadata().getFieldType("age", CQL_TYPE))
        .isEqualTo(GenericType.of(JsonNode.class));
  }

  @Test
  void handleByteBuffer() throws IOException {
    // given
    Schema schema = Schema.BYTES;
    byte[] bytes = "{\"name\":\"Bobby McGee\",\"age\":21}".getBytes(StandardCharsets.UTF_8);
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    Record<GenericRecord> record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", "{\"pk\":15}", byteBuffer, schema);

    LocalSchemaRegistry localSchemaRegistry = new LocalSchemaRegistry();
    PulsarSinkRecordImpl pulsarSinkRecordImpl =
        new PulsarSinkRecordImpl(record, localSchemaRegistry);

    InnerDataAndMetadata innerDataAndMetadataValue =
        MetadataCreator.makeMeta(pulsarSinkRecordImpl.value());

    InnerDataAndMetadata innerDataAndMetadataKey =
        MetadataCreator.makeMeta(pulsarSinkRecordImpl.key());

    assertThat(innerDataAndMetadataKey.getInnerData().getFieldValue("pk"))
        .isEqualTo(IntNode.valueOf(15));
    assertThat(innerDataAndMetadataKey.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadataKey.getInnerMetadata().getFieldType("pk", CQL_TYPE))
        .isEqualTo(GenericType.of(JsonNode.class));

    assertThat(innerDataAndMetadataValue.getInnerData().getFieldValue("name"))
        .isEqualTo(TextNode.valueOf("Bobby McGee"));
    assertThat(innerDataAndMetadataValue.getInnerData().getFieldValue("age"))
        .isEqualTo(IntNode.valueOf(21));
    assertThat(innerDataAndMetadataValue.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadataValue.getInnerMetadata().getFieldType("name", CQL_TYPE))
        .isEqualTo(GenericType.of(JsonNode.class));
    assertThat(innerDataAndMetadataValue.getInnerMetadata().getFieldType("age", CQL_TYPE))
        .isEqualTo(GenericType.of(JsonNode.class));
  }

  @Test
  void handleByteBufferStruct() throws IOException {
    // given
    Schema schema = Schema.AVRO(MyPojoWithBlob.class);
    GenericRecord object =
        new GenericRecordImpl()
            .put("name", "Bobby McGee")
            .put("age", 21)
            .put("blob", new byte[] {1, 2, 3, 4})
            .put("blobBuffer", ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
    Record<GenericRecord> record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, object, schema);

    LocalSchemaRegistry localSchemaRegistry = new LocalSchemaRegistry();
    PulsarSinkRecordImpl pulsarSinkRecordImpl =
        new PulsarSinkRecordImpl(record, localSchemaRegistry);

    InnerDataAndMetadata innerDataAndMetadata =
        MetadataCreator.makeMeta(pulsarSinkRecordImpl.value());

    // then
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("name")).isEqualTo("Bobby McGee");
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("age")).isEqualTo(21);
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("blob"))
        .isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
    assertThat(innerDataAndMetadata.getInnerData().getFieldValue("blobBuffer"))
        .isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
    assertThat(innerDataAndMetadata.getInnerMetadata()).isNotNull();
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("name", CQL_TYPE))
        .isEqualTo(GenericType.STRING);
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("age", CQL_TYPE))
        .isEqualTo(GenericType.INTEGER);
    // GenericType.BYTE_BUFFER in both cases, see StructData.getFieldValue
    // The driver requires a ByteBuffer rather than byte[] when inserting a blob.
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("blob", CQL_TYPE))
        .isEqualTo(GenericType.BYTE_BUFFER);
    assertThat(innerDataAndMetadata.getInnerMetadata().getFieldType("blobBuffer", CQL_TYPE))
        .isEqualTo(GenericType.BYTE_BUFFER);
  }
}
