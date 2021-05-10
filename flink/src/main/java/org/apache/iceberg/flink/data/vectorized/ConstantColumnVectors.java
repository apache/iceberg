/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.data.vectorized;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.vector.BooleanColumnVector;
import org.apache.flink.table.data.vector.BytesColumnVector;
import org.apache.flink.table.data.vector.DecimalColumnVector;
import org.apache.flink.table.data.vector.DoubleColumnVector;
import org.apache.flink.table.data.vector.FloatColumnVector;
import org.apache.flink.table.data.vector.IntColumnVector;
import org.apache.flink.table.data.vector.LongColumnVector;
import org.apache.flink.table.data.vector.TimestampColumnVector;

class ConstantColumnVectors {
  private ConstantColumnVectors() {
  }

  static IntOrcColumnVector ints(Object constant) {
    return new IntOrcColumnVector(constant);
  }

  static LongOrcColumnVector longs(Object constant) {
    return new LongOrcColumnVector(constant);
  }

  static BooleanOrcColumnVector booleans(Object constant) {
    return new BooleanOrcColumnVector(constant);
  }

  static DoubleOrcColumnVector doubles(Object constant) {
    return new DoubleOrcColumnVector(constant);
  }

  static FloatOrcColumnVector floats(Object constant) {
    return new FloatOrcColumnVector(constant);
  }

  static DecimalOrcColumnVector decimals(Object constant) {
    return new DecimalOrcColumnVector(constant);
  }

  static TimestampOrcColumnVector timestamps(Object constant) {
    return new TimestampOrcColumnVector(constant);
  }

  static BytesOrcColumnVector bytes(Object constant) {
    return new BytesOrcColumnVector(constant);
  }

  private static class IntOrcColumnVector implements IntColumnVector {

    private Object constant;

    private IntOrcColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return false;
    }

    @Override
    public int getInt(int i) {
      return (int) constant;
    }
  }

  private static class LongOrcColumnVector implements LongColumnVector {

    private Object constant;

    private LongOrcColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public long getLong(int i) {
      return (long) constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return false;
    }
  }

  private static class BooleanOrcColumnVector implements BooleanColumnVector {
    private Object constant;

    private BooleanOrcColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public boolean getBoolean(int i) {
      return (boolean) constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return false;
    }
  }

  private static class DoubleOrcColumnVector implements DoubleColumnVector {
    private Object constant;

    private DoubleOrcColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return false;
    }

    @Override
    public double getDouble(int i) {
      return (double) constant;
    }
  }

  private static class FloatOrcColumnVector implements FloatColumnVector {
    private Object constant;

    private FloatOrcColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public float getFloat(int i) {
      return (float) constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return false;
    }
  }

  private static class DecimalOrcColumnVector implements DecimalColumnVector {
    private Object constant;

    private DecimalOrcColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public DecimalData getDecimal(int i, int precision, int scale) {
      return (DecimalData) constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return false;
    }
  }

  private static class TimestampOrcColumnVector implements TimestampColumnVector {
    private Object constant;

    private TimestampOrcColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public TimestampData getTimestamp(int i, int precision) {
      return (TimestampData) constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return false;
    }
  }

  private static class BytesOrcColumnVector implements BytesColumnVector {
    private Object constant;

    private BytesOrcColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public Bytes getBytes(int i) {
      BinaryStringData str = (BinaryStringData) constant;
      return new Bytes(str.toBytes(), 0, str.getSizeInBytes());
    }

    @Override
    public boolean isNullAt(int i) {
      return false;
    }
  }
}
