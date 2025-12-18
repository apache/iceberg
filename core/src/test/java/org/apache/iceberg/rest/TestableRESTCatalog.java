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

import com.github.benmanes.caffeine.cache.Ticker;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.catalog.SessionCatalog;

class TestableRESTCatalog extends RESTCatalog {
  private final Ticker ticker;

  TestableRESTCatalog(
      SessionCatalog.SessionContext context,
      Function<Map<String, String>, RESTClient> clientBuilder,
      Ticker ticker) {
    super(context, clientBuilder);

    this.ticker = ticker;
  }

  @Override
  protected RESTSessionCatalog newSessionCatalog(
      Function<Map<String, String>, RESTClient> clientBuilder) {
    // This is called from RESTCatalog's constructor, 'ticker' member is not yet set, we have to
    // defer passing it to the session catalog.
    return new TestableRESTSessionCatalog(clientBuilder, null);
  }

  @Override
  public void initialize(String name, Map<String, String> props) {
    ((TestableRESTSessionCatalog) sessionCatalog()).setTicker(ticker);

    super.initialize(name, props);
  }
}
