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
  public static IcebergFunction<?, ?> fromString(String function) {
    switch (function) {
      case MaskAlphanum.NAME:
        return MaskAlphanum.get();
      case MaskToFixedValue.NAME:
        return MaskToFixedValue.get();
      case ReplaceWithNull.NAME:
        return ReplaceWithNull.get();
      case ShowFirst4.NAME:
        return ShowFirst4.get();
      case ShowLast4.NAME:
        return ShowLast4.get();
      case TruncateToYear.NAME:
        return TruncateToYear.get();
      case TruncateToMonth.NAME:
        return TruncateToMonth.get();
      case Sha256Global.NAME:
        return Sha256Global.get();
      case Sha256QueryLocal.NAME:
        return Sha256QueryLocal.get();
      default:
        return new UnknownFunction(function);
    }
  }

  /** Returns a {@code mask_alphanum} {@link IcebergFunction} for string types. */
  public static IcebergFunction<CharSequence, CharSequence> maskAlphanum() {
    return MaskAlphanum.get();
  }

  /** Returns a {@code mask_to_fixed_value} {@link IcebergFunction}. */
  public static <T> IcebergFunction<T, T> maskToFixedValue() {
    return MaskToFixedValue.get();
  }

  /** Returns a {@code replace_with_null} {@link IcebergFunction} for any type. */
  public static <T> IcebergFunction<T, T> replaceWithNull() {
    return ReplaceWithNull.get();
  }

  /** Returns a {@code show_first_4} {@link IcebergFunction} for string types. */
  public static IcebergFunction<CharSequence, CharSequence> showFirst4() {
    return ShowFirst4.get();
  }

  /** Returns a {@code show_last_4} {@link IcebergFunction} for string types. */
  public static IcebergFunction<CharSequence, CharSequence> showLast4() {
    return ShowLast4.get();
  }

  /** Returns a {@code truncate_to_year} {@link IcebergFunction} for date and timestamp types. */
  public static <T> IcebergFunction<T, T> truncateToYear() {
    return TruncateToYear.get();
  }

  /** Returns a {@code truncate_to_month} {@link IcebergFunction} for date and timestamp types. */
  public static <T> IcebergFunction<T, T> truncateToMonth() {
    return TruncateToMonth.get();
  }

  /** Returns a {@code sha_256_global} {@link IcebergFunction} for string, int, long and binary. */
  public static <T> IcebergFunction<T, T> sha256Global() {
    return Sha256Global.get();
  }

  /** Returns a {@code sha_256_query_local} {@link SaltedFunction}, salted per query. */
  public static <T> SaltedFunction<T, T> sha256QueryLocal() {
    return Sha256QueryLocal.get();
  }
}
