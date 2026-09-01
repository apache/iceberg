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
package org.apache.iceberg.arrow.vectorized;

/**
 * Tracks whether a value at an index is null and the Parquet definition level associated with it.
 * For simplicity and performance, it is expected that the setter methods {@link #setNull(int,
 * int)}, {@link #setNulls(int, int, int)}, {@link #setNotNull(int, int)} and {@link
 * #setNotNulls(int, int, int)} are invoked with monotonically increasing values for the index
 * parameter.
 */
public interface NullabilityHolder {

  int size();

  void setNull(int index, int definitionLevel);

  void setNotNull(int index, int definitionLevel);

  void setNulls(int startIndex, int num, int definitionLevel);

  void setNotNulls(int startIndex, int num, int definitionLevel);

  /** Returns 1 if null, 0 otherwise. */
  byte isNullAt(int index);

  int definitionLevelAt(int index);

  boolean hasNulls();

  int numNulls();

  void reset();
}
