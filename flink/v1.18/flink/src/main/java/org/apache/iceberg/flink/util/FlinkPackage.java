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

import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;

public class FlinkPackage {

  private static final AtomicReference<String> VERSION = new AtomicReference<>();
  public static final String FLINK_UNKNOWN_VERSION = "FLINK-UNKNOWN-VERSION";

  private FlinkPackage() {}

  /** Returns Flink version string like x.y.z */
  public static String version() {
    if (null == VERSION.get()) {
      String detectedVersion = null;
      try {
        detectedVersion = versionFromJar();
        // use unknown version in case exact implementation version can't be found from the jar
        // (this can happen if the DataStream class appears multiple times in the same classpath
        // such as with shading)
        detectedVersion = detectedVersion != null ? detectedVersion : FLINK_UNKNOWN_VERSION;
      } catch (Exception e) {
        detectedVersion = FLINK_UNKNOWN_VERSION;
      }
      VERSION.set(detectedVersion);
    }

    return VERSION.get();
  }

  @VisibleForTesting
  static String versionFromJar() {
    // Choose {@link DataStream} class because it is one of the core Flink API
    return DataStream.class.getPackage().getImplementationVersion();
  }

  @VisibleForTesting
  static void setVersion(String version) {
    VERSION.set(version);
  }
}
