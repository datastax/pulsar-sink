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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

public class CqlLogicalTypes {
  public static final String CQL_VARINT = "cql_varint";
  public static final String CQL_DECIMAL = "cql_decimal";
  public static final String CQL_DURATION = "cql_duration";

  public static class CqlDurationConversion extends Conversion<CqlDuration> {
    @Override
    public Class<CqlDuration> getConvertedType() {
      return CqlDuration.class;
    }

    @Override
    public String getLogicalTypeName() {
      return CQL_DURATION;
    }

    @Override
    public CqlDuration fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
      int months = (int) value.get(0);
      int days = (int) value.get(1);
      long nanoseconds = (long) value.get(2);
      return CqlDuration.newInstance(months, days, nanoseconds);
    }
  }

  public static class CqlDecimalConversion extends Conversion<BigDecimal> {
    @Override
    public Class<BigDecimal> getConvertedType() {
      return BigDecimal.class;
    }

    @Override
    public String getLogicalTypeName() {
      return CQL_DECIMAL;
    }

    @Override
    public BigDecimal fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
      ByteBuffer bb = (ByteBuffer) value.get(0);
      byte[] bytes = new byte[bb.remaining()];
      bb.duplicate().get(bytes);
      int scale = (int) value.get(1);
      return new BigDecimal(new BigInteger(bytes), scale);
    }
  }

  public static class CqlVarintConversion extends Conversion<BigInteger> {
    @Override
    public Class<BigInteger> getConvertedType() {
      return BigInteger.class;
    }

    @Override
    public String getLogicalTypeName() {
      return CQL_VARINT;
    }

    @Override
    public BigInteger fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
      byte[] arr = new byte[value.remaining()];
      value.duplicate().get(arr);
      return new BigInteger(arr);
    }
  }

  static class CqlDecimalLogicalType extends LogicalType {
    public CqlDecimalLogicalType() {
      super(CQL_DECIMAL);
    }
  }

  static class CqlDurationLogicalType extends LogicalType {
    public CqlDurationLogicalType() {
      super(CQL_DURATION);
    }
  }

  static class CqlVarintLogicalType extends LogicalType {
    public CqlVarintLogicalType() {
      super(CQL_VARINT);
    }
  }
}
