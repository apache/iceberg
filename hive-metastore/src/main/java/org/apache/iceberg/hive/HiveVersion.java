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
package org.apache.iceberg.hive;

import java.util.List;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;

public enum HiveVersion {
  HIVE_4(4),
  HIVE_3(3),
  HIVE_2(2),
  HIVE_1_2(1),
  NOT_SUPPORTED(0);

  private final int order;
  private static final HiveVersion current = calculate();

  HiveVersion(int order) {
    this.order = order;
  }

  public static HiveVersion current() {
    return current;
  }

  public static boolean min(HiveVersion other) {
    return current.order >= other.order;
  }

  private static HiveVersion calculate() {
    String version = HiveVersionInfo.getShortVersion();
    List<String> versions = Splitter.on('.').splitToList(version);
    switch (versions.get(0)) {
      case "4":
        return HIVE_4;
      case "3":
        return HIVE_3;
      case "2":
        return HIVE_2;
      case "1":
        if (versions.get(1).equals("2")) {
          return HIVE_1_2;
        } else {
          return NOT_SUPPORTED;
        }
      default:
        return NOT_SUPPORTED;
    }
  }
}
