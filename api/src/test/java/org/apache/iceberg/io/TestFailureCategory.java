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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestFailureCategory {

  @ParameterizedTest
  @CsvSource({
    "401,AUTH",
    "403,AUTH",
    "404,NOT_FOUND",
    "429,THROTTLED",
    "500,TRANSIENT",
    "503,TRANSIENT",
    "599,TRANSIENT"
  })
  public void mapsKnownStatusCodes(int status, FailureCategory expected) {
    assertThat(FailureCategory.fromHttpStatus(status)).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({"200", "400", "418", "499", "600", "0", "-1"})
  public void unmappedStatusCodesAreUnknown(int status) {
    assertThat(FailureCategory.fromHttpStatus(status)).isEqualTo(FailureCategory.UNKNOWN);
  }
}
