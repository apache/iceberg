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
package org.apache.iceberg.spark.actions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.iceberg.util.HilbertByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Combines per-column ordered bytes (produced by {@link SparkZOrderUDF#sortedLexicographically})
 * into a single Hilbert-curve value. Mirrors the combine step of {@link SparkZOrderUDF}
 * (interleave), replacing it with {@link HilbertByteUtils#hilbertIndex}.
 */
class SparkHilbertUDF implements Serializable {

  private transient ThreadLocal<ByteBuffer> outputBuffer;

  private final int numCols;
  private final int bitsPerColumn;
  private final int outputBytes;

  SparkHilbertUDF(int numCols, int bitsPerColumn) {
    this.numCols = numCols;
    this.bitsPerColumn = bitsPerColumn;
    this.outputBytes = numCols * (bitsPerColumn / 8);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    outputBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(outputBytes));
  }

  private byte[] hilbertValue(Seq<byte[]> scalaBinary) {
    byte[][] columnsBinary = JavaConverters.seqAsJavaList(scalaBinary).toArray(new byte[numCols][]);
    return HilbertByteUtils.hilbertIndex(columnsBinary, bitsPerColumn, outputBuffer.get());
  }

  private final UserDefinedFunction hilbertUDF =
      functions
          .udf((Seq<byte[]> arrayBinary) -> hilbertValue(arrayBinary), DataTypes.BinaryType)
          .withName("HILBERT_BYTES");

  Column hilbertValue(Column arrayBinary) {
    return hilbertUDF.apply(arrayBinary);
  }
}
