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

import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;

/**
 * Writer function for the generic Iceberg {@link Record} data model.
 *
 * <p>Converts Iceberg Records to Maps of column name to value, suitable for the Lance Arrow
 * conversion pipeline.
 */
public class GenericLanceWriter {

  private GenericLanceWriter() {}

  /**
   * Creates a function that converts Iceberg Records to a Map representation.
   *
   * @param icebergSchema the Iceberg schema defining the columns
   * @param arrowSchema the corresponding Arrow schema (unused for generic records)
   * @return a function mapping Records to column name/value Maps
   */
  public static Function<Record, Map<String, Object>> create(
      Schema icebergSchema, org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
    return record -> LanceArrowConverter.recordToMap(record, icebergSchema);
  }
}
