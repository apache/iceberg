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
package org.apache.iceberg.flink.procedure;

import org.apache.flink.table.procedures.Procedure;
import org.apache.iceberg.catalog.Catalog;

/**
 * Service Provider Interface (SPI) for discovering and creating {@link Procedure} implementations.
 *
 * <p>Each implementation acts as a provider for a specific procedure type and must be registered
 * in:
 *
 * <pre><code>resources/META-INF/services/org.apache.iceberg.flink.procedure.ProcedureService</code>
 * </pre>
 *
 * <p>The file must contain the fully qualified class name of the ProviderService implementation.
 */
public interface ProcedureService {
  /**
   * Returns a procedure name which is used to create a given procedure.
   *
   * <p>For example: sql "CALL sys.create_tag(tableId, tagName)" will provide "create_tag" name
   * which will be used by {@link ProcedureUtil}'s registry to map this to {@link
   * CreateTagProcedure} class.
   */
  String procedureName();

  /**
   * Creates a specific procedure within specific catalog. This method used in {@link ProcedureBase}
   * to instantiate a necessary procedure via reflection mechanism.
   */
  Procedure create(Catalog catalog);
}
