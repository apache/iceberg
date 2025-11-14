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
package org.apache.iceberg.rest;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.ViewMetadata;

/**
 * A factory interface for creating {@link RESTTableOperations} and {@link RESTViewOperations}
 * instances for REST catalogs.
 *
 * <p>This interface allows custom implementations of table and view operations to be injected into
 * {@link RESTSessionCatalog} and {@link RESTCatalog}, enabling extensibility for specialized use
 * cases.
 *
 * <p>Example usage:
 *
 * <pre>
 * RESTOperationsFactory customFactory = new RESTOperationsFactory() {
 *   {@literal @}Override
 *   public RESTTableOperations createTableOperations(
 *       RESTClient client,
 *       String path,
 *       Supplier&lt;Map&lt;String, String&gt;&gt; headers,
 *       FileIO io,
 *       TableMetadata current,
 *       Set&lt;Endpoint&gt; endpoints) {
 *     return new CustomRESTTableOperations(client, path, headers, io, current, endpoints);
 *   }
 *
 *   {@literal @}Override
 *   public RESTViewOperations createViewOperations(
 *       RESTClient client,
 *       String path,
 *       Supplier&lt;Map&lt;String, String&gt;&gt; headers,
 *       ViewMetadata current,
 *       Set&lt;Endpoint&gt; endpoints) {
 *     return new CustomRESTViewOperations(client, path, headers, current, endpoints);
 *   }
 * };
 *
 * RESTSessionCatalog catalog = new RESTSessionCatalog(clientBuilder, ioBuilder, customFactory);
 * </pre>
 */
public interface RESTOperationsFactory {

  /** Default {@link RESTOperationsFactory} instance. */
  RESTOperationsFactory DEFAULT = new RESTOperationsFactory() {};

  /**
   * Create a new {@link RESTTableOperations} instance for simple table operations.
   *
   * <p>The default implementation creates a standard {@link RESTTableOperations} instance.
   *
   * @param client the REST client to use for communicating with the catalog server
   * @param path the REST path for the table
   * @param headers a supplier for additional HTTP headers to include in requests
   * @param io the FileIO implementation for reading and writing table metadata and data files
   * @param current the current table metadata
   * @param endpoints the set of supported REST endpoints
   * @return a new RESTTableOperations instance
   */
  default RESTTableOperations createTableOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      FileIO io,
      TableMetadata current,
      Set<Endpoint> endpoints) {
    return new RESTTableOperations(client, path, headers, io, current, endpoints);
  }

  /**
   * Create a new {@link RESTTableOperations} instance for transaction-based operations (create or
   * replace).
   *
   * <p>This method is used when creating tables or replacing table metadata within a transaction.
   * The default implementation creates a standard {@link RESTTableOperations} instance.
   *
   * @param client the REST client to use for communicating with the catalog server
   * @param path the REST path for the table
   * @param headers a supplier for additional HTTP headers to include in requests
   * @param io the FileIO implementation for reading and writing table metadata and data files
   * @param updateType the type of update being performed (CREATE, REPLACE, or SIMPLE)
   * @param createChanges the list of metadata updates to apply during table creation or replacement
   * @param current the current table metadata (may be null for CREATE operations)
   * @param endpoints the set of supported REST endpoints
   * @return a new RESTTableOperations instance
   */
  default RESTTableOperations createTableOperationsForTransaction(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      FileIO io,
      RESTTableOperations.UpdateType updateType,
      List<MetadataUpdate> createChanges,
      TableMetadata current,
      Set<Endpoint> endpoints) {
    return new RESTTableOperations(
        client, path, headers, io, updateType, createChanges, current, endpoints);
  }

  /**
   * Create a new {@link RESTViewOperations} instance.
   *
   * <p>The default implementation creates a standard {@link RESTViewOperations} instance.
   *
   * @param client the REST client to use for communicating with the catalog server
   * @param path the REST path for the view
   * @param headers a supplier for additional HTTP headers to include in requests
   * @param current the current view metadata
   * @param endpoints the set of supported REST endpoints
   * @return a new RESTViewOperations instance
   */
  default RESTViewOperations createViewOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      ViewMetadata current,
      Set<Endpoint> endpoints) {
    return new RESTViewOperations(client, path, headers, current, endpoints);
  }
}
