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

public final class AuthProperties {

  private AuthProperties() {}

  public static final String AUTH_TYPE = "rest.auth.type";

  public static final String AUTH_TYPE_NONE = "none";
  public static final String AUTH_TYPE_BASIC = "basic";
  public static final String AUTH_TYPE_OAUTH2 = "oauth2";
  public static final String AUTH_TYPE_SIGV4 = "sigv4";
  public static final String AUTH_TYPE_GOOGLE = "google";

  public static final String AUTH_MANAGER_IMPL_NONE =
      "org.apache.iceberg.rest.auth.NoopAuthManager";
  public static final String AUTH_MANAGER_IMPL_BASIC =
      "org.apache.iceberg.rest.auth.BasicAuthManager";
  public static final String AUTH_MANAGER_IMPL_OAUTH2 =
      "org.apache.iceberg.rest.auth.OAuth2Manager";
  public static final String AUTH_MANAGER_IMPL_SIGV4 =
      "org.apache.iceberg.aws.RESTSigV4AuthManager";
  public static final String AUTH_MANAGER_IMPL_GOOGLE =
      "org.apache.iceberg.gcp.auth.GoogleAuthManager";

  public static final String BASIC_USERNAME = "rest.auth.basic.username";
  public static final String BASIC_PASSWORD = "rest.auth.basic.password";

  public static final String SIGV4_DELEGATE_AUTH_TYPE = "rest.auth.sigv4.delegate-auth-type";
  public static final String SIGV4_DELEGATE_AUTH_TYPE_DEFAULT = AUTH_TYPE_OAUTH2;
}
