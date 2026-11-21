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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/** Helpers for benchmarks that measure the device scan against Iceberg's own readers. */
public final class GpuBenchmarks {

  private GpuBenchmarks() {}

  /**
   * Materializes every live row of the table with all of its top-level columns on the host and
   * reads every value, folding them into {@code checksum} so that none can be skipped: the work
   * Spark does to build every row of a scan. Returns the live row count.
   */
  public static long materializeAll(
      DeviceMergeOnReadScan scan, Schema schema, AtomicLong checksum) {
    List<String> columns = Lists.newArrayList();
    for (Types.NestedField field : schema.columns()) {
      columns.add(field.name());
    }
    return scan.materialize(
        columns,
        batch -> {
          int count = columns.size();
          int[] index = new int[count];
          char[] kind = new char[count];
          for (int c = 0; c < count; c++) {
            Types.NestedField field = schema.findField(columns.get(c));
            index[c] = batch.column(field.name());
            kind[c] =
                switch (field.type().typeId()) {
                  case INTEGER, DATE -> 'i';
                  case LONG, TIMESTAMP, DECIMAL -> 'l';
                  case FLOAT, DOUBLE -> 'd';
                  case STRING -> 's';
                  default -> throw new UnsupportedOperationException("Column " + field);
                };
          }
          // Column by column, each loop specialised to its type: every value is still read.
          long sum = 0;
          int rows = batch.rows();
          for (int c = 0; c < count; c++) {
            int column = index[c];
            long part = 0;
            switch (kind[c]) {
              case 'i' -> {
                for (int r = 0; r < rows; r++) {
                  part = part * 17 + (batch.isNull(column, r) ? 31 : batch.getInt(column, r));
                }
              }
              case 'l' -> {
                for (int r = 0; r < rows; r++) {
                  part = part * 17 + (batch.isNull(column, r) ? 31 : batch.getLong(column, r));
                }
              }
              case 'd' -> {
                for (int r = 0; r < rows; r++) {
                  part =
                      part * 17
                          + (batch.isNull(column, r)
                              ? 31
                              : Double.doubleToRawLongBits(batch.getDouble(column, r)));
                }
              }
              default -> {
                for (int r = 0; r < rows; r++) {
                  ByteBuffer utf8 = batch.getUtf8(column, r);
                  part = part * 17 + (utf8 == null ? 31 : utf8Hash(utf8));
                }
              }
            }
            sum = sum * 31 + part;
          }
          checksum.addAndGet(sum);
        });
  }

  /** Every byte of a string, without decoding it -- as Spark reads a UTF8String. */
  private static long utf8Hash(ByteBuffer bytes) {
    long hash = 0;
    for (int i = bytes.position(); i < bytes.limit(); i++) {
      hash = hash * 31 + bytes.get(i);
    }
    return hash;
  }
}
