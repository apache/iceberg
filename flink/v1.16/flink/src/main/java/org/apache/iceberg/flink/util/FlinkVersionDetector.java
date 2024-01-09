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

public class FlinkVersionDetector {
  public String version() {
    String version = null;
    try {
      version = getVersionFromJar();
    } catch (Exception e) {
      /* we can't detect the exact implementation version from the jar (this can happen if the DataStream class
       appears multiple times in the same classpath such as with shading), so the best we can do is say it's
       1.16.x below
      */
    }
    if (version == null) {
      version = "1.16.x";
    }
    return version;
  }

  String getVersionFromJar() {
    /* Choose {@link DataStream} class because it is one of the core Flink API. */
    return DataStream.class.getPackage().getImplementationVersion();
  }
}
