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
package org.apache.iceberg.hashicorp;

import java.io.Serializable;

public final class VaultProperties implements Serializable {
  /** Vault server address */
  public static final String VAULT_ADDRESS_PROP = "vault.address";

  /** Direct token authentication */
  public static final String VAULT_TOKEN_PROP = "vault.token";

  /** Transit secrets engine mount path */
  public static final String VAULT_TRANSIT_MOUNT_PROP = "vault.transit-mount";

  /** AppRole authentication path */
  public static final String VAULT_APPROLE_PATH_PROP = "vault.approle-path";

  /** Role ID for AppRole authentication */
  public static final String VAULT_ROLE_ID_PROP = "vault.role-id";

  /** Secret ID for AppRole authentication */
  public static final String VAULT_SECRET_ID_PROP = "vault.secret-id";

  /** Environment variable of Secret ID for AppRole authentication */
  public static final String VAULT_SECRET_ID_ENV_VAR = "VAULT_SECRET_ID";

  /** Enable token rotation for AppRole authentication */
  public static final String VAULT_ROTATE_TOKEN_PROP = "vault.rotate-token";

  private VaultProperties() {}
}
