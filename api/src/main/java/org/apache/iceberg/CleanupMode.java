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
package org.apache.iceberg;

/** An enum representing possible clean up mode used in snapshot expiration. */
public enum CleanupMode {
  NONE(0),
  METADATA_ONLY(1),
  ALL(2);

  CleanupMode(int id) {
    this.id = id;
  }

  private final int id;

  public int id() {
    return id;
  }

  public boolean requireCleanup() {
    return this.id > 0;
  }

  public static CleanupMode fromId(int id) {
    switch (id) {
      case 0:
        return NONE;
      case 1:
        return METADATA_ONLY;
      case 2:
        return ALL;
    }
    throw new IllegalArgumentException("Unknown cleanup mode: " + id);
  }
}
