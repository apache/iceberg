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

import java.util.function.Supplier;

public class ValuesReaderFactory {

  private static Supplier<ValuesAsBytesReader> unsafeValuesReaderClass;

  public static ValuesAsBytesReader getReader() {
    if (unsafeValuesReaderClass != null) {
      return unsafeValuesReaderClass.get();
    } else {
      return new ValuesAsBytesReader();
    }
  }

  public static synchronized void initUnsafeReaderIfAbsent(
      Supplier<ValuesAsBytesReader> unsafeReaderClass) {
    if (unsafeValuesReaderClass != null) {
      return;
    }
    unsafeValuesReaderClass = unsafeReaderClass;
  }

  private ValuesReaderFactory() {}
}
