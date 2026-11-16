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

import java.io.Serializable;

final class MappingResult implements Serializable {
  private final String location;
  private final MappingRule rule;

  private MappingResult(String location, MappingRule rule) {
    this.location = location;
    this.rule = rule;
  }

  static MappingResult mapped(String location, MappingRule rule) {
    return new MappingResult(location, rule);
  }

  static MappingResult unmatched(String location) {
    return new MappingResult(location, null);
  }

  String location() {
    return location;
  }

  MappingRule rule() {
    return rule;
  }

  boolean mapped() {
    return rule != null;
  }
}
