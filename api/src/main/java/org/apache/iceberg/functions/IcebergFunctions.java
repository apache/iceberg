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
package org.apache.iceberg.functions;

/** Factory for the {@link IcebergFunction} implementations defined by the REST spec. */
public class IcebergFunctions {
  private IcebergFunctions() {}

  /**
   * Returns the function with the given name, or an {@link UnknownFunction} if the name is not
   * recognized.
   */
  public static IcebergFunction<?, ?> fromString(String function, int fieldId) {
    switch (function) {
      case MaskAlphanum.NAME:
        return maskAlphanum(fieldId);
      case MaskToFixedValue.NAME:
        return maskToFixedValue(fieldId);
      case ReplaceWithNull.NAME:
        return replaceWithNull(fieldId);
      case ShowFirst4.NAME:
        return showFirst4(fieldId);
      case ShowLast4.NAME:
        return showLast4(fieldId);
      case TruncateToYear.NAME:
        return truncateToYear(fieldId);
      case TruncateToMonth.NAME:
        return truncateToMonth(fieldId);
      case Sha256Global.NAME:
        return sha256Global(fieldId);
      case Sha256QueryLocal.NAME:
        return sha256QueryLocal(fieldId);
      default:
        return new UnknownFunction(fieldId, function);
    }
  }

  /** Returns a {@code mask-alphanum} {@link IcebergFunction} for string types. */
  public static IcebergFunction<String, String> maskAlphanum(int fieldId) {
    return new MaskAlphanum(fieldId);
  }

  /** Returns a {@code mask-to-fixed-value} {@link IcebergFunction}. */
  public static IcebergFunction<Object, Object> maskToFixedValue(int fieldId) {
    return new MaskToFixedValue(fieldId);
  }

  /** Returns a {@code replace-with-null} {@link IcebergFunction} for any type. */
  public static IcebergFunction<Object, Object> replaceWithNull(int fieldId) {
    return new ReplaceWithNull(fieldId);
  }

  /** Returns a {@code show-first-4} {@link IcebergFunction} for string types. */
  public static IcebergFunction<String, String> showFirst4(int fieldId) {
    return new ShowFirst4(fieldId);
  }

  /** Returns a {@code show-last-4} {@link IcebergFunction} for string types. */
  public static IcebergFunction<String, String> showLast4(int fieldId) {
    return new ShowLast4(fieldId);
  }

  /** Returns a {@code truncate-to-year} {@link IcebergFunction} for date and timestamp types. */
  public static IcebergFunction<Object, Object> truncateToYear(int fieldId) {
    return new TruncateToYear(fieldId);
  }

  /** Returns a {@code truncate-to-month} {@link IcebergFunction} for date and timestamp types. */
  public static IcebergFunction<Object, Object> truncateToMonth(int fieldId) {
    return new TruncateToMonth(fieldId);
  }

  /** Returns a {@code sha-256-global} {@link IcebergFunction} for string, int, long and binary. */
  public static IcebergFunction<Object, Object> sha256Global(int fieldId) {
    return new Sha256Global(fieldId);
  }

  /** Returns a {@code sha-256-query-local} {@link SaltedFunction}, salted per query. */
  public static SaltedFunction<Object, Object> sha256QueryLocal(int fieldId) {
    return new Sha256QueryLocal(fieldId);
  }
}
