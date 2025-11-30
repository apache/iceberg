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

import org.apache.arrow.vector.NullCheckingForGet;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.parquet.ParquetFormatModel;

public class ArrowFormatModels {
  public static void register() {
    FormatModelRegistry.register(
        new ParquetFormatModel<>(
            ColumnarBatch.class,
            Object.class,
            (schema, messageType, constantValues, properties) ->
                ArrowReader.VectorizedCombinedScanIterator.buildReader(
                    schema,
                    messageType, /* setArrowValidityVector */
                    NullCheckingForGet.NULL_CHECKING_ENABLED)));
  }

  private ArrowFormatModels() {}
}
