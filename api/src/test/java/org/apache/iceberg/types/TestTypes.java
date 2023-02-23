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
package org.apache.iceberg.types;

import org.junit.Assert;
import org.junit.Test;

public class TestTypes {

  @Test
  public void fromPrimitiveString() {
    Assert.assertSame(Types.BooleanType.get(), Types.fromPrimitiveString("boolean"));
    Assert.assertSame(Types.BooleanType.get(), Types.fromPrimitiveString("BooLean"));

    Assert.assertSame(Types.TimestampType.withoutZone(), Types.fromPrimitiveString("timestamp"));

    Assert.assertEquals(Types.FixedType.ofLength(3), Types.fromPrimitiveString("Fixed[ 3 ]"));

    Assert.assertEquals(Types.DecimalType.of(2, 3), Types.fromPrimitiveString("Decimal( 2 , 3 )"));

    Assert.assertEquals(Types.DecimalType.of(2, 3), Types.fromPrimitiveString("Decimal(2,3)"));

    IllegalArgumentException exception =
        Assert.assertThrows(
            IllegalArgumentException.class, () -> Types.fromPrimitiveString("Unknown"));
    Assert.assertTrue(exception.getMessage().contains("Unknown"));
  }
}
