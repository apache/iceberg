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
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.iceberg.io.FileIO;

class TestableRESTSessionCatalog extends RESTSessionCatalog {
  private Ticker ticker;

  TestableRESTSessionCatalog(
      Function<Map<String, String>, RESTClient> clientBuilder,
      BiFunction<SessionContext, Map<String, String>, FileIO> ioBuilder) {
    super(clientBuilder, ioBuilder);
  }

  public void setTicker(Ticker newTicker) {
    this.ticker = newTicker;
  }

  @Override
  protected RESTTableCache createTableCache(Map<String, String> props) {
    return new RESTTableCache(props, ticker);
  }
}
