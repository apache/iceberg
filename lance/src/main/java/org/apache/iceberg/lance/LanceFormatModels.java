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

import org.apache.iceberg.data.Record;
import org.apache.iceberg.formats.FormatModelRegistry;

/**
 * Registration entry point for Lance format models.
 *
 * <p>This class is auto-discovered by {@link FormatModelRegistry} via reflection when the
 * iceberg-lance jar is on the classpath. The {@code register()} method is called during static
 * initialization of the registry.
 */
public class LanceFormatModels {

  /**
   * Registers Lance format models for the generic Record data model.
   *
   * <p>Registers two models:
   *
   * <ul>
   *   <li>A data/equality-delete model using {@link Record} as the data type
   *   <li>A position-delete model using {@link org.apache.iceberg.deletes.PositionDelete}
   * </ul>
   */
  public static void register() {
    FormatModelRegistry.register(
        LanceFormatModel.create(
            Record.class,
            Void.class,
            (icebergSchema, arrowSchema, engineSchema) ->
                GenericLanceWriter.create(icebergSchema, arrowSchema),
            (icebergSchema, arrowSchema, engineSchema, idToConstant) ->
                GenericLanceReader.buildReader(icebergSchema, arrowSchema, idToConstant)));

    FormatModelRegistry.register(LanceFormatModel.forPositionDeletes());

    // Register Spark-specific format models when Spark is on the classpath
    try {
      Class.forName("org.apache.spark.sql.catalyst.InternalRow");
      org.apache.iceberg.lance.spark.SparkLanceFormatModels.register();
    } catch (ClassNotFoundException e) {
      // Spark not on classpath, skip Spark registrations
    }
  }

  private LanceFormatModels() {}
}
