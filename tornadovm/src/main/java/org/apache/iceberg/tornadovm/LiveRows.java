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
package org.apache.iceberg.tornadovm;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** The live rows of one data file after merge-on-read, column by name. */
public interface LiveRows {
  int rows();

  /** The index of a column, for the by-index accessors: resolve once, read many rows. */
  int column(String name);

  boolean isNull(int column, int row);

  int getInt(int column, int row);

  long getLong(int column, int row);

  double getDouble(int column, int row);

  /** A string column's value, whatever its length; null when the row's value is null. */
  String getString(int column, int row);

  /**
   * A string column's value as its UTF-8 bytes, without decoding or copying them -- what Spark's
   * UTF8String holds; null when the row's value is null.
   */
  default ByteBuffer getUtf8(int column, int row) {
    String value = getString(column, row);
    return value == null ? null : ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
  }

  default boolean isNull(String column, int row) {
    return isNull(column(column), row);
  }

  default int getInt(String column, int row) {
    return getInt(column(column), row);
  }

  default long getLong(String column, int row) {
    return getLong(column(column), row);
  }

  default double getDouble(String column, int row) {
    return getDouble(column(column), row);
  }

  default String getString(String column, int row) {
    return getString(column(column), row);
  }

  /** A string column's live values: UTF-8 bytes, and each row's start and length in them. */
  record Utf8Column(byte[] bytes, int[] starts, int[] lengths) {}

  /** Copies an INT32 or DATE column's values of all rows into {@code into}. */
  default void copyInts(int column, int[] into) {
    for (int r = 0; r < rows(); r++) {
      into[r] = isNull(column, r) ? 0 : getInt(column, r);
    }
  }

  /** Copies an INT64 or TIMESTAMP column's values of all rows into {@code into}. */
  default void copyLongs(int column, long[] into) {
    for (int r = 0; r < rows(); r++) {
      into[r] = isNull(column, r) ? 0 : getLong(column, r);
    }
  }

  /** Copies a floating-point column's values of all rows into {@code into}. */
  default void copyDoubles(int column, double[] into) {
    for (int r = 0; r < rows(); r++) {
      into[r] = isNull(column, r) ? 0 : getDouble(column, r);
    }
  }

  /** Marks the null rows of a column in {@code into}; returns whether there were any. */
  default boolean copyNulls(int column, boolean[] into) {
    boolean any = false;
    for (int r = 0; r < rows(); r++) {
      into[r] = isNull(column, r);
      any |= into[r];
    }
    return any;
  }

  /** A string column's values, without a String a row. */
  default Utf8Column utf8Column(int column) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    int[] starts = new int[rows()];
    int[] lengths = new int[rows()];
    for (int r = 0; r < rows(); r++) {
      ByteBuffer value = getUtf8(column, r);
      starts[r] = bytes.size();
      if (value != null) {
        lengths[r] = value.remaining();
        bytes.write(value.array(), value.arrayOffset() + value.position(), value.remaining());
      }
    }
    return new Utf8Column(bytes.toByteArray(), starts, lengths);
  }
}
