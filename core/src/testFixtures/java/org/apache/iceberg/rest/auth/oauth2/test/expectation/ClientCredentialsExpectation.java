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
package org.apache.iceberg.rest.auth.oauth2.test.expectation;

import com.nimbusds.oauth2.sdk.GrantType;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.auth.oauth2.test.TestServer;
import org.immutables.value.Value;
import org.mockserver.model.Parameter;

@Value.Immutable
@SuppressWarnings("resource")
public abstract class ClientCredentialsExpectation extends TokenEndpointExpectation {

  @Override
  public void create() {
    TestServer.instance().when(request()).respond(response("access_initial", "refresh_initial"));
  }

  @Override
  protected ImmutableList.Builder<Parameter> requestBody() {
    return super.requestBody()
        .add(param("grant_type", GrantType.CLIENT_CREDENTIALS))
        .add(param("scope", ACCEPTED_SCOPES));
  }
}
