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
package org.apache.iceberg.parquet;

import org.apache.parquet.io.api.Binary;

public interface TripleWriter<T> {
  // TODO: should definition level be included, or should it be part of the column?

  /**
   * Write a value.
   *
   * @param rl repetition level
   * @param value the value
   */
  void write(int rl, T value);

  /**
   * Write a triple.
   *
   * @param rl repetition level
   * @param value the boolean value
   */
  default void writeBoolean(int rl, boolean value) {
    throw new UnsupportedOperationException("Not a boolean column");
  }

  /**
   * Write a triple.
   *
   * @param rl repetition level
   * @param value the boolean value
   */
  default void writeInteger(int rl, int value) {
    throw new UnsupportedOperationException("Not an integer column");
  }

  /**
   * Write a triple.
   *
   * @param rl repetition level
   * @param value the boolean value
   */
  default void writeLong(int rl, long value) {
    throw new UnsupportedOperationException("Not an long column");
  }

  /**
   * Write a triple.
   *
   * @param rl repetition level
   * @param value the boolean value
   */
  default void writeFloat(int rl, float value) {
    throw new UnsupportedOperationException("Not an float column");
  }

  /**
   * Write a triple.
   *
   * @param rl repetition level
   * @param value the boolean value
   */
  default void writeDouble(int rl, double value) {
    throw new UnsupportedOperationException("Not an double column");
  }

  /**
   * Write a triple.
   *
   * @param rl repetition level
   * @param value the boolean value
   */
  default void writeBinary(int rl, Binary value) {
    throw new UnsupportedOperationException("Not an binary column");
  }

  /**
   * Write a triple for a null value.
   *
   * @param rl repetition level
   * @param dl definition level
   */
  void writeNull(int rl, int dl);
}
