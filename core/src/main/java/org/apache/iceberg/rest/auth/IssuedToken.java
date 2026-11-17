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
package org.apache.iceberg.rest.auth;

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public final class IssuedToken {
  private final String value;
  private final Long expiresAtMillis;

  private IssuedToken(String value, Long expiresAtMillis) {
    this.value = value;
    this.expiresAtMillis = expiresAtMillis;
  }

  public static IssuedToken of(String value, Long expiresAtMillis) {
    Preconditions.checkArgument(
        value != null && !value.isEmpty(), "Invalid token value: null or empty");
    return new IssuedToken(value, expiresAtMillis);
  }

  public String value() {
    return value;
  }

  public Long expiresAtMillis() {
    return expiresAtMillis;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("value", "****")
        .add("expiresAtMillis", expiresAtMillis)
        .toString();
  }
}
