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

/**
 * Internal properties used by the REST catalog and its FileIO implementations.
 *
 * <p>These properties are not intended for user consumption, and may be removed or changed in
 * future releases without notice.
 */
public final class RESTCatalogInternalProperties {

  private RESTCatalogInternalProperties() {}

  public static final String PREFIX = "rest.internal.";

  /**
   * The (fully-qualified) table identifier, percent-encoded as per RFC 3986.
   *
   * <p>This property is automatically set by the REST catalog client when creating table-scoped
   * FileIO instances, and is intended for consumption by FileIO internal components, such as
   * request signers.
   *
   * <p>The identifier is created by joining together the {@link
   * org.apache.iceberg.catalog.TableIdentifier} parts using the catalog's configured {@linkplain
   * RESTCatalogProperties#NAMESPACE_SEPARATOR namespace separator}, then by percent-encoding the
   * result.
   */
  public static final String TABLE_IDENTIFIER = PREFIX + "table-id";

  /**
   * The remote signing configuration, JSON-encoded.
   *
   * <p>This property is automatically set by FileIO implementations when creating table-scoped
   * request signers.
   *
   * <p>It carries the {@link org.apache.iceberg.rest.signing.RemoteSigningConfig} object returned
   * by the catalog in the {@link org.apache.iceberg.rest.responses.LoadTableResponse}.
   */
  public static final String REMOTE_SIGNING_CONFIG = PREFIX + "remote-signing-config";
}
