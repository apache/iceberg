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
package org.apache.iceberg.rest.auth;

import java.lang.reflect.Field;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Util.AuthSession;
import org.junit.jupiter.api.Assertions;

/** Helper class to make the token refresh retries configurable for testing */
public class AuthSessionUtil {

  private AuthSessionUtil() {}

  public static void setTokenRefreshNumRetries(int retries) {
    AuthSession.setTokenRefreshNumRetries(retries);
  }

  public static AuthSession getCatalogAuthSession(RESTCatalog catalog) {
    try {
      Field scf = catalog.getClass().getDeclaredField("sessionCatalog");
      scf.setAccessible(true);
      RESTSessionCatalog sc = (RESTSessionCatalog) scf.get(catalog);
      Field caf = sc.getClass().getDeclaredField("catalogAuth");
      caf.setAccessible(true);
      return (AuthSession) caf.get(sc);
    } catch (Exception e) {
      return Assertions.fail("Failed to get AuthSession from RESTCatalog", e);
    }
  }
}
