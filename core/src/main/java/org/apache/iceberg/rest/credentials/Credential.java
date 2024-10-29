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
package org.apache.iceberg.rest.credentials;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.immutables.value.Value;

@Value.Immutable
public interface Credential {
  String prefix();

  Map<String, String> config();

  @Value.Check
  default void validate() {
    Preconditions.checkArgument(!prefix().isEmpty(), "Invalid prefix: must be non-empty");
    Preconditions.checkArgument(!config().isEmpty(), "Invalid config: must be non-empty");
  }
}
