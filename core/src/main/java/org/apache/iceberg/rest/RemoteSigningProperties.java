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
 * Remote signing properties.
 *
 * <p>These properties are automatically set by the REST catalog client when creating table-scoped
 * FileIO instances, and are intended for consumption by remote request signers.
 *
 * <p>They are not intended for user-facing configuration, and may be removed or changed in future
 * releases without notice.
 */
public final class RemoteSigningProperties {

  private RemoteSigningProperties() {}

  public static final String PREFIX = "rest.remote-signing.";

  /**
   * The remote signing endpoint path, as computed by {@link
   * ResourcePaths#remoteSign(org.apache.iceberg.catalog.TableIdentifier)}.
   */
  public static final String ENDPOINT = PREFIX + "endpoint";

  /** The {@link org.apache.iceberg.rest.signing.RemoteSigningConfig}, JSON-encoded. */
  public static final String CONFIG = PREFIX + "config";
}
