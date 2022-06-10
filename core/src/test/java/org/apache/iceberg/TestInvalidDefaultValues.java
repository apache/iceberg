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

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TestDefaultValuesParsingAndUnParsing.defaultValueParseAndUnParseRoundTrip;

public class TestInvalidDefaultValues {

  @Test
  public void testInvalidFixed() {
    Type expectedType = Types.FixedType.ofLength(2);
    String defaultJson = "\"111ff\"";
    Assert.assertThrows(IllegalArgumentException.class, () -> defaultValueParseAndUnParseRoundTrip(
        expectedType,
        defaultJson));
  }

  @Test
  public void testInvalidUUID() {
    Type expectedType = Types.FixedType.ofLength(2);
    String defaultJson = "\"eb26bdb1-a1d8-4aa6-990e-da940875492c-abcde\"";
    Assert.assertThrows(IllegalArgumentException.class, () -> defaultValueParseAndUnParseRoundTrip(
        expectedType,
        defaultJson));
  }

  @Test
  public void testInvalidMap() {
    Type expectedType = Types.MapType.ofOptional(1, 2, Types.IntegerType.get(), Types.StringType.get());
    String defaultJson = "{\"keys\": [1, 2, 3], \"values\": [\"foo\", \"bar\"]}";
    Assert.assertThrows(IllegalArgumentException.class, () -> defaultValueParseAndUnParseRoundTrip(
        expectedType,
        defaultJson));
  }

  @Test
  public void testInvalidDecimal() {
    Type expectedType = Types.DecimalType.of(5, 2);
    String defaultJson = "123.456";
    Assert.assertThrows(IllegalArgumentException.class, () -> defaultValueParseAndUnParseRoundTrip(
        expectedType,
        defaultJson));
  }
}
