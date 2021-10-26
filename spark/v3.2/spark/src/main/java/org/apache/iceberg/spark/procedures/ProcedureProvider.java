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

package org.apache.iceberg.spark.procedures;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Provides a set of procedures to be registered in a {@link org.apache.iceberg.spark.SparkCatalog
 * SparkCatalog} or {@link org.apache.iceberg.spark.SparkSessionCatalog SparkSessionCatalog} using
 * the {@link Namespace namespace} returned by {@link #namespace()}.
 *
 * <p>Instances of this class are discovered using Java's {@link java.util.ServiceLoader}
 * mechanism.
 */
public interface ProcedureProvider {

  /**
   * Name of this {@link ProcedureProvider}.
   *
   * @return (technical) name of this provider.
   */
  String name();

  /**
   * Human-readable description of this {@link ProcedureProvider}.
   *
   * @return human-readable description.
   */
  String description();

  /**
   * Namespace under which the procedures provided by this {@link ProcedureProvider} shall be
   * accessible.
   *
   * @return the namespace for the procedures provided by this {@link ProcedureProvider}.
   */
  Namespace namespace();

  /**
   * Returns a map of procedure names to suppliers of {@link ProcedureBuilder}s.
   *
   * @param catalogName       The name of the catalog.
   * @param options           The configuration options of the {@link org.apache.iceberg.spark.SparkCatalog
   *                          SparkCatalog} or {@link org.apache.iceberg.spark.SparkSessionCatalog
   *                          SparkSessionCatalog}.
   * @param forSessionCatalog Flag whether the procedures are built for a {@link
   *                          org.apache.iceberg.spark.SparkCatalog SparkCatalog} ({@code false}) or
   *                          {@link org.apache.iceberg.spark.SparkSessionCatalog
   *                          SparkSessionCatalog} ({@code true}).
   * @return map of procedure name to {@link ProcedureBuilder} suppliers. May return {@code null} or
   * an empty map as well.
   */
  Map<String, Supplier<ProcedureBuilder<?>>> createProcedureBuilders(String catalogName,
      CaseInsensitiveStringMap options, boolean forSessionCatalog);
}
