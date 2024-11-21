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
package org.apache.iceberg.aws;

import java.util.Map;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.OAuth2Manager;
import org.apache.iceberg.rest.auth.OAuth2Util;

/**
 * An AuthManager that authenticates requests with SigV4.
 *
 * <p>It extends {@link OAuth2Manager} to handle OAuth2 authentication as well. In case of
 * conflicting headers, the OAuth2 Authorization header will be relocated, then included in the
 * canonical headers to sign.
 *
 * <p>See <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html">Signing AWS
 * API requests</a> for details about the SigV4 protocol.
 */
@SuppressWarnings("unused") // loaded by reflection
public class RESTSigV4AuthManager extends OAuth2Manager {

  private RESTSigV4Signer signer;

  public RESTSigV4AuthManager(String name) {
    super(name);
  }

  @Override
  public RESTSigv4AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
    RESTSigV4Signer initSigner = new RESTSigV4Signer(properties);
    return new RESTSigv4AuthSession(super.initSession(initClient, properties), initSigner);
  }

  @Override
  public RESTSigv4AuthSession catalogSession(
      RESTClient sharedClient, Map<String, String> properties) {
    signer = new RESTSigV4Signer(properties);
    return new RESTSigv4AuthSession(super.catalogSession(sharedClient, properties), signer);
  }

  @Override
  protected RESTSigv4AuthSession newSessionFromAccessToken(
      String token, Map<String, String> properties, OAuth2Util.AuthSession parent) {
    return new RESTSigv4AuthSession(
        super.newSessionFromAccessToken(token, properties, parent), signer);
  }

  @Override
  protected RESTSigv4AuthSession newSessionFromCredential(
      String credential, OAuth2Util.AuthSession parent) {
    return new RESTSigv4AuthSession(super.newSessionFromCredential(credential, parent), signer);
  }

  @Override
  protected RESTSigv4AuthSession newSessionFromTokenExchange(
      String token, String tokenType, OAuth2Util.AuthSession parent) {
    return new RESTSigv4AuthSession(
        super.newSessionFromTokenExchange(token, tokenType, parent), signer);
  }
}
