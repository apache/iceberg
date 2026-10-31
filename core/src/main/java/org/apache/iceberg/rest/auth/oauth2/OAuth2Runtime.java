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
package org.apache.iceberg.rest.auth.oauth2;

import com.nimbusds.oauth2.sdk.http.HTTPRequestSender;
import java.time.Clock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.util.ThreadPools;

/**
 * Dependencies provided by the environment rather than by the user configuration: the HTTP client
 * used to contact the authorization server, the executor used to renew tokens, and the clock.
 */
record OAuth2Runtime(HTTPRequestSender httpClient, ScheduledExecutorService executor, Clock clock) {

  /** Creates a default runtime for the given {@link RESTClient} supplier. */
  OAuth2Runtime(Supplier<RESTClient> restClientSupplier) {
    this(
        new RESTClientAdapter(restClientSupplier),
        ThreadPools.authRefreshPool(),
        Clock.systemUTC());
  }
}
