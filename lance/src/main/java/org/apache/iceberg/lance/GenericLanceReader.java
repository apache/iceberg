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
package org.apache.iceberg.lance;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Reader function for the generic Iceberg {@link Record} data model.
 *
 * <p>Converts Arrow VectorSchemaRoot batches from Lance files into Iceberg Records.
 */
public class GenericLanceReader {

  private GenericLanceReader() {}

  /**
   * Creates a function that converts Arrow batches into CloseableIterables of Iceberg Records.
   *
   * @param icebergSchema the Iceberg schema to construct Records with
   * @param arrowSchema the Arrow schema of the Lance file (unused, schema comes from batch)
   * @param idToConstant constant values to inject (e.g., partition values)
   * @return a function that takes a (batch, idToConstant) Entry and produces Records
   */
  public static Function<Map.Entry<VectorSchemaRoot, Map<Integer, ?>>, CloseableIterable<Record>>
      buildReader(
          Schema icebergSchema,
          org.apache.arrow.vector.types.pojo.Schema arrowSchema,
          Map<Integer, ?> idToConstant) {
    return entry -> {
      VectorSchemaRoot batch = entry.getKey();
      Map<Integer, ?> constants = entry.getValue();

      List<Record> records = Lists.newArrayListWithCapacity(batch.getRowCount());
      for (int i = 0; i < batch.getRowCount(); i++) {
        records.add(LanceArrowConverter.readRow(batch, i, icebergSchema, constants));
      }

      return new CloseableIterable<Record>() {
        @Override
        public CloseableIterator<Record> iterator() {
          return CloseableIterator.withClose(records.iterator());
        }

        @Override
        public void close() throws IOException {
          // batch lifecycle managed by ArrowReader
        }
      };
    };
  }
}
