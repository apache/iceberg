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

import com.nimbusds.oauth2.sdk.GrantType;
import java.util.concurrent.CompletionStage;

/**
 * An interface representing an OAuth2 flow.
 *
 * <p>A flow is a complete sequence of interactions (generally one, but sometimes more) between the
 * client, the authorization server, and (optionally) the resource owner (for human-to-machine
 * flows) in order to obtain an access token.
 */
public interface Flow {

  /** Returns the OAuth2 grant type used by this flow. Useful mostly for logging purposes. */
  GrantType grantType();

  /**
   * Executes the flow and returns a {@link CompletionStage} that completes when the flow is done
   * and new tokens are available.
   *
   * <p>A flow may be stateful or stateless. Stateful flows should clean up internal resources when
   * the returned {@link CompletionStage} completes.
   *
   * @return A stage that completes when new tokens are fetched.
   */
  CompletionStage<TokensResult> execute();
}
