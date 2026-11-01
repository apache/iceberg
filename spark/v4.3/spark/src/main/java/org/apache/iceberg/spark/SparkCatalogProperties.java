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
package org.apache.iceberg.spark;

public final class SparkCatalogProperties {

  private SparkCatalogProperties() {}

  /**
   * Controls whether Spark should delegate DROP TABLE PURGE requests to the REST catalog instead of
   * performing client-side file deletion.
   *
   * <p>When enabled, Spark sends the purge request to the REST catalog, allowing the catalog to
   * handle deletion.
   *
   * <p>Defaults to false for backward compatibility.
   */
  public static final String REST_CATALOG_PURGE = "rest-catalog-purge";

  public static final boolean REST_CATALOG_PURGE_DEFAULT = false;
}
