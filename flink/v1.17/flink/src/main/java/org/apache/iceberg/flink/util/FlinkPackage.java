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
package org.apache.iceberg.flink.util;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;

public class FlinkPackage {

  public static final String FLINK_UNKNOWN_VERSION = "Flink-UNKNOWN";

  private FlinkPackage() {}

  /** Returns Flink version string like x.y.z */
  public static String version() {
    try {
      String version = getVersionFromJar();
      /* If we can't detect the exact implementation version from the jar (this can happen if the DataStream class
       appears multiple times in the same classpath such as with shading), then the best we can do is say it's
       unknown
      */
      return version != null ? version : FLINK_UNKNOWN_VERSION;
    } catch (Exception e) {
      return FLINK_UNKNOWN_VERSION;
    }
  }

  @VisibleForTesting
  static String getVersionFromJar() {
    /* Choose {@link DataStream} class because it is one of the core Flink API. */
    return DataStream.class.getPackage().getImplementationVersion();
  }
}
