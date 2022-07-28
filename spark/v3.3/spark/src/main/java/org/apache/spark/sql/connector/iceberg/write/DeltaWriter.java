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
package org.apache.spark.sql.connector.iceberg.write;

import java.io.IOException;
import org.apache.spark.sql.connector.write.DataWriter;

/** A data writer responsible for writing a delta of rows. */
public interface DeltaWriter<T> extends DataWriter<T> {
  /**
   * Passes information for a row that must be deleted.
   *
   * @param metadata values for metadata columns that were projected but are not part of the row ID
   * @param id a row ID to delete
   * @throws IOException if the write process encounters an error
   */
  void delete(T metadata, T id) throws IOException;

  /**
   * Passes information for a row that must be updated together with the updated row.
   *
   * @param metadata values for metadata columns that were projected but are not part of the row ID
   * @param id a row ID to update
   * @param row a row with updated values
   * @throws IOException if the write process encounters an error
   */
  void update(T metadata, T id, T row) throws IOException;

  /**
   * Passes a row to insert.
   *
   * @param row a row to insert
   * @throws IOException if the write process encounters an error
   */
  void insert(T row) throws IOException;

  @Override
  default void write(T row) throws IOException {
    insert(row);
  }
}
