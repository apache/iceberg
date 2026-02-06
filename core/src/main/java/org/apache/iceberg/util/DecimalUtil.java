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
package org.apache.iceberg.util;

import java.math.BigDecimal;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class DecimalUtil {
  private DecimalUtil() {}

  /**
   * Convert a {@link BigDecimal} to reused fix length bytes, the extra bytes are filled according
   * to the signum.
   */
  public static byte[] toReusedFixLengthBytes(
      int precision, int scale, BigDecimal decimal, byte[] reuseBuf) {
    Preconditions.checkArgument(
        decimal.scale() == scale,
        "Cannot write value as decimal(%s,%s), wrong scale: %s",
        precision,
        scale,
        decimal);
    Preconditions.checkArgument(
        decimal.precision() <= precision,
        "Cannot write value as decimal(%s,%s), too large: %s",
        precision,
        scale,
        decimal);

    byte[] unscaled = decimal.unscaledValue().toByteArray();
    if (unscaled.length == reuseBuf.length) {
      return unscaled;
    }

    byte fillByte = (byte) (decimal.signum() < 0 ? 0xFF : 0x00);
    int offset = reuseBuf.length - unscaled.length;

    for (int i = 0; i < reuseBuf.length; i += 1) {
      if (i < offset) {
        reuseBuf[i] = fillByte;
      } else {
        reuseBuf[i] = unscaled[i - offset];
      }
    }

    return reuseBuf;
  }
}
