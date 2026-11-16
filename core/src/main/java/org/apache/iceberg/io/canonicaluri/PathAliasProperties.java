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
package org.apache.iceberg.io.canonicaluri;

/** Configuration properties for {@link CanonicalUriFileIO}. */
public final class PathAliasProperties {
  /** Enables or disables location mapping. */
  public static final String ENABLED = "pathalias.enabled";

  /** Default value for {@link #ENABLED}. */
  public static final boolean ENABLED_DEFAULT = true;

  /** Prefix for indexed mapping-rule properties. */
  public static final String RULE_PREFIX = "pathalias.rule.";

  /** Delegate {@code FileIO} implementation class. */
  public static final String DELEGATE_IMPL = "pathalias.delegate.impl";

  /** Default value for {@link #DELEGATE_IMPL}. */
  public static final String DELEGATE_IMPL_DEFAULT = "org.apache.iceberg.io.ResolvingFileIO";

  /** Policy for locations that do not match a rule. */
  public static final String UNMATCHED = "pathalias.unmatched";

  /** Default value for {@link #UNMATCHED}. */
  public static final String UNMATCHED_DEFAULT = "passthrough";

  /** Write mapping policy. */
  public static final String WRITE_MODE = "pathalias.write-mode";

  /** Default value for {@link #WRITE_MODE}. */
  public static final String WRITE_MODE_DEFAULT = "strict";

  /** Enables storage-specific read failover when supported. */
  public static final String READ_FAILOVER = "pathalias.read-failover";

  /** Default value for {@link #READ_FAILOVER}. */
  public static final boolean READ_FAILOVER_DEFAULT = false;

  /** Enables operation counters. */
  public static final String METRICS_ENABLED = "pathalias.metrics.enabled";

  /** Default value for {@link #METRICS_ENABLED}. */
  public static final boolean METRICS_ENABLED_DEFAULT = true;

  /** Enables physical locations in debug log messages. */
  public static final String DEBUG_LOG_PHYSICAL_PATHS = "pathalias.debug.log-physical-paths";

  /** Default value for {@link #DEBUG_LOG_PHYSICAL_PATHS}. */
  public static final boolean DEBUG_LOG_PHYSICAL_PATHS_DEFAULT = false;

  /** Canonical namespace enforced by {@link CanonicalLocationProvider}. */
  public static final String LOCATION_PROVIDER_CANONICAL_PREFIX =
      "pathalias.location-provider.canonical-prefix";

  private PathAliasProperties() {}
}
