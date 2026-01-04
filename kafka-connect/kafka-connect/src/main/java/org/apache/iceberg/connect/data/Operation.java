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
package org.apache.iceberg.connect.data;

import java.util.Locale;

public enum Operation {

  // Note: Enums have no stable hash code across different JVMs, use toByteValue() for
  // this purpose.

  /** Insertion operation. */
  INSERT("C", (byte) 0),

  /** Update operation with the previous content of the updated row. */
  UPDATE("U", (byte) 1),

  /** Deletion operation. */
  DELETE("D", (byte) 2);

  private final String shortString;

  private final byte value;

  /**
   * Creates a {@link Operation} enum with the given short string and byte value representation of
   * the {@link Operation}.
   */
  Operation(String shortString, byte value) {
    this.shortString = shortString;
    this.value = value;
  }

  /**
   * Returns a short string representation of this {@link Operation}.
   *
   * <p>
   *
   * <ul>
   *   <li>"R,C" represents {@link #INSERT}.
   *   <li>"U" represents {@link #UPDATE}.
   *   <li>"D" represents {@link #DELETE}.
   * </ul>
   */
  public String shortString() {
    return shortString;
  }

  /**
   * Returns the byte value representation of this {@link Operation}. The byte value is used for
   * serialization and deserialization.
   *
   * <p>
   *
   * <ul>
   *   <li>"0" represents {@link #INSERT}.
   *   <li>"1" represents {@link #UPDATE}.
   *   <li>"2" represents {@link #DELETE}.
   * </ul>
   */
  public byte toByteValue() {
    return value;
  }

  /**
   * Creates a {@link Operation} from the given byte value. Each {@link Operation} has a byte value
   * representation.
   *
   * @see #toByteValue()
   */
  public static Operation fromByteValue(byte value) {
    switch (value) {
      case 0:
        return INSERT;
      case 1:
        return UPDATE;
      case 2:
        return DELETE;
      default:
        throw new UnsupportedOperationException(
            "Unsupported byte value '" + value + "' for row kind.");
    }
  }

  public static Operation fromString(String shortString) {
    switch (shortString.toUpperCase(Locale.ROOT)) {
      case "C":
      case "R":
        return INSERT;
      case "U":
        return UPDATE;
      case "D":
        return DELETE;
      default:
        throw new UnsupportedOperationException(
            "Unsupported short string '" + shortString + "' for row kind.");
    }
  }
}
