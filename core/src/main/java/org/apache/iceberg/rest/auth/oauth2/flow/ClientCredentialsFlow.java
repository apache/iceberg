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
package org.apache.iceberg.rest.auth.oauth2.flow;

import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ClientCredentialsGrant;
import com.nimbusds.oauth2.sdk.GrantType;
import java.util.concurrent.CompletionStage;
import org.immutables.value.Value;

/**
 * An implementation of the <a
 * href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.4">Client Credentials Grant</a>
 * flow.
 */
@Value.Immutable
abstract class ClientCredentialsFlow extends BaseFlow {

  private static final AuthorizationGrant GRANT = new ClientCredentialsGrant();

  interface Builder extends BaseFlow.Builder<ClientCredentialsFlow, Builder> {}

  @Override
  public final GrantType grantType() {
    return GrantType.CLIENT_CREDENTIALS;
  }

  @Override
  public CompletionStage<TokensResult> execute() {
    return invokeTokenEndpoint(GRANT);
  }
}
