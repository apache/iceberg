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

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

/**
 * Iceberg REST Compatibility Kit
 *
 * <p>This test suite provides the ability to run the Iceberg catalog tests against a remote REST
 * catalog implementation to verify the behaviors against the reference implementation catalog
 * tests.
 *
 * <p>The tests can be configured through environment variables or system properties. By default,
 * the tests will run using a local http server using a servlet implementation that leverages the
 * {@link RESTCatalogAdapter}.
 */
@Suite
@SuiteDisplayName("Iceberg REST Compatibility Kit")
@SelectClasses({RESTCompatibilityKitCatalogTests.class, RESTCompatibilityKitViewCatalogTests.class})
public class RESTCompatibilityKitSuite {
  static final String RCK_REQUIRES_NAMESPACE_CREATE = "rck.requires-namespace-create";
  static final String RCK_SUPPORTS_SERVERSIDE_RETRY = "rck.supports-serverside-retry";
  static final String RCK_OVERRIDES_REQUESTED_LOCATION = "rck.overrides-requested-location";

  protected RESTCompatibilityKitSuite() {}
}
