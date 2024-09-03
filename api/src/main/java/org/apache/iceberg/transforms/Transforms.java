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
package org.apache.iceberg.transforms;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;

/**
 * Factory methods for transforms.
 *
 * <p>Most users should create transforms using a {@link PartitionSpec#builderFor(Schema)} partition
 * spec builder}.
 *
 * @see PartitionSpec#builderFor(Schema) The partition spec builder.
 */
public class Transforms {
  private Transforms() {}

  private static final Pattern HAS_WIDTH = Pattern.compile("(\\w+)\\[(\\d+)\\]");

  public static Transform<?, ?> fromString(String transform) {
    Matcher widthMatcher = HAS_WIDTH.matcher(transform);
    if (widthMatcher.matches()) {
      String name = widthMatcher.group(1);
      int parsedWidth = Integer.parseInt(widthMatcher.group(2));
      if (name.equalsIgnoreCase("truncate")) {
        return Truncate.get(parsedWidth);
      } else if (name.equalsIgnoreCase("bucket")) {
        return Bucket.get(parsedWidth);
      }
    }

    if (transform.equalsIgnoreCase("identity")) {
      return Identity.get();
    } else if (transform.equalsIgnoreCase("year")) {
      return Years.get();
    } else if (transform.equalsIgnoreCase("month")) {
      return Months.get();
    } else if (transform.equalsIgnoreCase("day")) {
      return Days.get();
    } else if (transform.equalsIgnoreCase("hour")) {
      return Hours.get();
    } else if (transform.equalsIgnoreCase("void")) {
      return VoidTransform.get();
    }

    return new UnknownTransform<>(transform);
  }

  /** @deprecated use {@link #identity()} instead; will be removed in 2.0.0 */
  @Deprecated
  public static Transform<?, ?> fromString(Type type, String transform) {
    Matcher widthMatcher = HAS_WIDTH.matcher(transform);
    if (widthMatcher.matches()) {
      String name = widthMatcher.group(1);
      int parsedWidth = Integer.parseInt(widthMatcher.group(2));
      if (name.equalsIgnoreCase("truncate")) {
        return (Transform<?, ?>) Truncate.get(type, parsedWidth);
      } else if (name.equalsIgnoreCase("bucket")) {
        return (Transform<?, ?>) Bucket.get(type, parsedWidth);
      }
    }

    String lowerTransform = transform.toLowerCase(Locale.ENGLISH);
    switch (lowerTransform) {
      case "identity":
        return Identity.get(type);
      case "year":
        return Years.get().toEnum(type);
      case "month":
        return Months.get().toEnum(type);
      case "day":
        return Days.get().toEnum(type);
      case "hour":
        return Hours.get().toEnum(type);
      case "void":
        return VoidTransform.get();
    }

    return new UnknownTransform<>(transform);
  }

  /**
   * Returns an identity {@link Transform} that can be used for any type.
   *
   * @param type the {@link Type source type} for the transform
   * @param <T> Java type passed to this transform
   * @return an identity transform
   * @deprecated use {@link #identity()} instead; will be removed in 2.0.0
   */
  @Deprecated
  public static <T> Transform<T, T> identity(Type type) {
    return Identity.get(type);
  }

  /**
   * Returns a year {@link Transform} for date or timestamp types.
   *
   * @param type the {@link Type source type} for the transform
   * @param <T> Java type passed to this transform
   * @return a year transform
   * @deprecated use {@link #year()} instead; will be removed in 2.0.0
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public static <T> Transform<T, Integer> year(Type type) {
    return (Transform<T, Integer>) Years.get().toEnum(type);
  }

  /**
   * Returns a month {@link Transform} for date or timestamp types.
   *
   * @param type the {@link Type source type} for the transform
   * @param <T> Java type passed to this transform
   * @return a month transform
   * @deprecated use {@link #month()} instead; will be removed in 2.0.0
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public static <T> Transform<T, Integer> month(Type type) {
    return (Transform<T, Integer>) Months.get().toEnum(type);
  }

  /**
   * Returns a day {@link Transform} for date or timestamp types.
   *
   * @param type the {@link Type source type} for the transform
   * @param <T> Java type passed to this transform
   * @return a day transform
   * @deprecated use {@link #day()} instead; will be removed in 2.0.0
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public static <T> Transform<T, Integer> day(Type type) {
    return (Transform<T, Integer>) Days.get().toEnum(type);
  }

  /**
   * Returns an hour {@link Transform} for timestamps.
   *
   * @param type the {@link Type source type} for the transform
   * @param <T> Java type passed to this transform
   * @return an hour transform
   * @deprecated use {@link #hour()} instead; will be removed in 2.0.0
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public static <T> Transform<T, Integer> hour(Type type) {
    return (Transform<T, Integer>) Hours.get().toEnum(type);
  }

  /**
   * Returns a bucket {@link Transform} for the given type and number of buckets.
   *
   * @param type the {@link Type source type} for the transform
   * @param numBuckets the number of buckets for the transform to produce
   * @param <T> Java type passed to this transform
   * @return a transform that buckets values into numBuckets
   * @deprecated use {@link #bucket(int)} instead; will be removed in 2.0.0
   */
  @Deprecated
  public static <T> Transform<T, Integer> bucket(Type type, int numBuckets) {
    return Bucket.get(type, numBuckets);
  }

  /**
   * Returns a truncate {@link Transform} for the given type and width.
   *
   * @param type the {@link Type source type} for the transform
   * @param width the width to truncate data values
   * @param <T> Java type passed to this transform
   * @return a transform that truncates the given type to width
   * @deprecated use {@link #truncate(int)} instead; will be removed in 2.0.0
   */
  @Deprecated
  public static <T> Transform<T, T> truncate(Type type, int width) {
    return (Transform<T, T>) Truncate.get(type, width);
  }

  /**
   * Returns an identity {@link Transform} that can be used for any type.
   *
   * @param <T> Java type passed to this transform
   * @return an identity transform
   */
  public static <T> Transform<T, T> identity() {
    return Identity.get();
  }

  /**
   * Returns a year {@link Transform} for date or timestamp types.
   *
   * @param <T> Java type passed to this transform
   * @return a year transform
   */
  public static <T> Transform<T, Integer> year() {
    return Years.get();
  }

  /**
   * Returns a month {@link Transform} for date or timestamp types.
   *
   * @param <T> Java type passed to this transform
   * @return a month transform
   */
  public static <T> Transform<T, Integer> month() {
    return Months.get();
  }

  /**
   * Returns a day {@link Transform} for date or timestamp types.
   *
   * @param <T> Java type passed to this transform
   * @return a day transform
   */
  public static <T> Transform<T, Integer> day() {
    return Days.get();
  }

  /**
   * Returns an hour {@link Transform} for timestamp types.
   *
   * @param <T> Java type passed to this transform
   * @return an hour transform
   */
  public static <T> Transform<T, Integer> hour() {
    return Hours.get();
  }

  /**
   * Returns a bucket {@link Transform} for the given number of buckets.
   *
   * @param numBuckets the number of buckets for the transform to produce
   * @param <T> Java type passed to this transform
   * @return a transform that buckets values into numBuckets
   */
  public static <T> Transform<T, Integer> bucket(int numBuckets) {
    return Bucket.get(numBuckets);
  }

  /**
   * Returns a truncate {@link Transform} for the given width.
   *
   * @param width the width to truncate data values
   * @param <T> Java type passed to this transform
   * @return a transform that truncates the given type to width
   */
  public static <T> Transform<T, T> truncate(int width) {
    return Truncate.get(width);
  }

  /**
   * Returns a {@link Transform} that always produces null.
   *
   * @param <T> Java type accepted by the transform.
   * @return a transform that always produces null (the void transform).
   */
  public static <T> Transform<T, Void> alwaysNull() {
    return VoidTransform.get();
  }
}
