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

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.junit.EnumLike;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

class TestClientCredentialsFlow {

  @CartesianTest
  void fetchNewTokens(@EnumLike(excludes = "none") ClientAuthenticationMethod authenticationMethod)
      throws InterruptedException, ExecutionException {
    try (TestEnvironment env =
        TestEnvironment.builder().clientAuthenticationMethod(authenticationMethod).build()) {
      FlowFactory flowFactory = env.newFlowFactory();
      Flow flow = flowFactory.newInitialFlow();
      assertThat(flow).isInstanceOf(ClientCredentialsFlow.class);
      TokensResult tokens = flow.execute().toCompletableFuture().get();
      env.assertTokensResult(tokens, "access_initial", null);
    }
  }
}
