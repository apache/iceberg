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
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class RandomUtil {

  private RandomUtil() {
  }

  private static boolean negate(int num) {
    return num % 2 == 1;
  }

  @SuppressWarnings("RandomModInteger")
  public static Object generatePrimitive(Type.PrimitiveType primitive,
                                         Random random) {
    int choice = random.nextInt(20);

    switch (primitive.typeId()) {
      case BOOLEAN:
        return choice < 10;

      case INTEGER:
        switch (choice) {
          case 1:
            return Integer.MIN_VALUE;
          case 2:
            return Integer.MAX_VALUE;
          case 3:
            return 0;
          default:
            return negate(choice) ? -1 * random.nextInt() : random.nextInt();
        }

      case LONG:
        switch (choice) {
          case 1:
            return Long.MIN_VALUE;
          case 2:
            return Long.MAX_VALUE;
          case 3:
            return 0L;
          default:
            return negate(choice) ? -1L * random.nextLong() : random.nextLong();
        }

      case FLOAT:
        switch (choice) {
          case 1:
            return Float.MIN_VALUE;
          case 2:
            return -Float.MIN_VALUE;
          case 3:
            return Float.MAX_VALUE;
          case 4:
            return -Float.MAX_VALUE;
          case 5:
            return Float.NEGATIVE_INFINITY;
          case 6:
            return Float.POSITIVE_INFINITY;
          case 7:
            return 0.0F;
          case 8:
            return Float.NaN;
          default:
            return negate(choice) ? -1.0F * random.nextFloat() : random.nextFloat();
        }

      case DOUBLE:
        switch (choice) {
          case 1:
            return Double.MIN_VALUE;
          case 2:
            return -Double.MIN_VALUE;
          case 3:
            return Double.MAX_VALUE;
          case 4:
            return -Double.MAX_VALUE;
          case 5:
            return Double.NEGATIVE_INFINITY;
          case 6:
            return Double.POSITIVE_INFINITY;
          case 7:
            return 0.0D;
          case 8:
            return Double.NaN;
          default:
            return negate(choice) ? -1.0D * random.nextDouble() : random.nextDouble();
        }

      case DATE:
        // this will include negative values (dates before 1970-01-01)
        return random.nextInt() % ABOUT_380_YEARS_IN_DAYS;

      case TIME:
        return (random.nextLong() & Integer.MAX_VALUE) % ONE_DAY_IN_MICROS;

      case TIMESTAMP:
        return random.nextLong() % FIFTY_YEARS_IN_MICROS;

      case STRING:
        return randomString(random);

      case UUID:
        byte[] uuidBytes = new byte[16];
        random.nextBytes(uuidBytes);
        // this will hash the uuidBytes
        return uuidBytes;

      case FIXED:
        byte[] fixed = new byte[((Types.FixedType) primitive).length()];
        random.nextBytes(fixed);
        return fixed;

      case BINARY:
        byte[] binary = new byte[random.nextInt(50)];
        random.nextBytes(binary);
        return binary;

      case DECIMAL:
        Types.DecimalType type = (Types.DecimalType) primitive;
        BigInteger unscaled = randomUnscaled(type.precision(), random);
        BigDecimal bigDecimal = new BigDecimal(unscaled, type.scale());
        return negate(choice) ? bigDecimal.negate() : bigDecimal;

      default:
        throw new IllegalArgumentException(
            "Cannot generate random value for unknown type: " + primitive);
    }
  }

  public static Object generateDictionaryEncodablePrimitive(Type.PrimitiveType primitive, Random random) {
    int value = random.nextInt(3);
    switch (primitive.typeId()) {
      case BOOLEAN:
        return true; // doesn't really matter for booleans since they are not dictionary encoded
      case INTEGER:
      case DATE:
        return negate(value) ? -1 * value : value;
      case FLOAT:
        return negate(value) ? -1.0F * (float) value : (float) value;
      case DOUBLE:
        return negate(value) ? -1.0D * (double) value : (double) value;
      case LONG:
      case TIME:
      case TIMESTAMP:
        return (long) value;
      case STRING:
        return String.valueOf(value);
      case FIXED:
        byte[] fixed = new byte[((Types.FixedType) primitive).length()];
        Arrays.fill(fixed, (byte) value);
        return fixed;
      case BINARY:
        byte[] binary = new byte[value + 1];
        Arrays.fill(binary, (byte) value);
        return binary;
      case DECIMAL:
        Types.DecimalType type = (Types.DecimalType) primitive;
        BigInteger unscaled = new BigInteger(String.valueOf(value + 1));
        BigDecimal bd = new BigDecimal(unscaled, type.scale());
        return negate(value) ? bd.negate() : bd;
      default:
        throw new IllegalArgumentException(
            "Cannot generate random value for unknown type: " + primitive);
    }
  }

  private static final long FIFTY_YEARS_IN_MICROS =
      (50L * (365 * 3 + 366) * 24 * 60 * 60 * 1_000_000) / 4;
  private static final int ABOUT_380_YEARS_IN_DAYS = 380 * 365;
  private static final long ONE_DAY_IN_MICROS = 24 * 60 * 60 * 1_000_000L;
  private static final String CHARS =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.!?";

  private static String randomString(Random random) {
    int length = random.nextInt(50);
    byte[] buffer = new byte[length];

    for (int i = 0; i < length; i += 1) {
      buffer[i] = (byte) CHARS.charAt(random.nextInt(CHARS.length()));
    }

    return new String(buffer);
  }

  private static final String DIGITS = "0123456789";

  private static BigInteger randomUnscaled(int precision, Random random) {
    int length = random.nextInt(precision);
    if (length == 0) {
      return BigInteger.ZERO;
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i += 1) {
      sb.append(DIGITS.charAt(random.nextInt(DIGITS.length())));
    }

    return new BigInteger(sb.toString());
  }
}
