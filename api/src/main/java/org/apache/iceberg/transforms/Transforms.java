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

import com.google.common.base.Preconditions;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;

/**
 * Factory methods for transforms.
 * <p>
 * Most users should create transforms using a
 * {@link PartitionSpec#builderFor(Schema)} partition spec builder}.
 *
 * @see PartitionSpec#builderFor(Schema) The partition spec builder.
 */
public class Transforms {
  private Transforms() {
  }

  private static final Pattern HAS_WIDTH = Pattern.compile("(\\w+)\\[(\\d+)\\]");

  public static Transform<?, ?> fromString(Type type, String transform) {
    Matcher widthMatcher = HAS_WIDTH.matcher(transform);
    if (widthMatcher.matches()) {
      String name = widthMatcher.group(1);
      int parsedWidth = Integer.parseInt(widthMatcher.group(2));
      if (name.equalsIgnoreCase("truncate")) {
        return Truncate.get(type, parsedWidth);
      } else if (name.equals("bucket")) {
        return Bucket.get(type, parsedWidth);
      }
    }

    if (transform.equalsIgnoreCase("identity")) {
      return Identity.get(type);
    }

    try {
      if (type.typeId() == Type.TypeID.TIMESTAMP) {
        return Timestamps.valueOf(transform.toUpperCase(Locale.ENGLISH));
      } else if (type.typeId() == Type.TypeID.DATE) {
        return Dates.valueOf(transform.toUpperCase(Locale.ENGLISH));
      }
    } catch (IllegalArgumentException ignored) {
      // fall through to return unknown transform
    }

    return new UnknownTransform<>(type, transform);
  }

  /**
   * Returns an identity {@link Transform} that can be used for any type.
   *
   * @param type the {@link Type source type} for the transform
   * @param <T> Java type passed to this transform
   * @return an identity transform
   */
  public static <T> Transform<T, T> identity(Type type) {
    return Identity.get(type);
  }

  /**
   * Returns a year {@link Transform} for date or timestamp types.
   *
   * @param type the {@link Type source type} for the transform
   * @param <T> Java type passed to this transform
   * @return a year transform
   */
  @SuppressWarnings("unchecked")
  public static <T> Transform<T, Integer> year(Type type) {
    switch (type.typeId()) {
      case DATE:
        return (Transform<T, Integer>) Dates.YEAR;
      case TIMESTAMP:
        return (Transform<T, Integer>) Timestamps.YEAR;
      default:
        throw new IllegalArgumentException(
            "Cannot partition type " + type + " by year");
    }
  }

  /**
   * Returns a month {@link Transform} for date or timestamp types.
   *
   * @param type the {@link Type source type} for the transform
   * @param <T> Java type passed to this transform
   * @return a month transform
   */
  @SuppressWarnings("unchecked")
  public static <T> Transform<T, Integer> month(Type type) {
    switch (type.typeId()) {
      case DATE:
        return (Transform<T, Integer>) Dates.MONTH;
      case TIMESTAMP:
        return (Transform<T, Integer>) Timestamps.MONTH;
      default:
        throw new IllegalArgumentException(
            "Cannot partition type " + type + " by month");
    }
  }

  /**
   * Returns a day {@link Transform} for date or timestamp types.
   *
   * @param type the {@link Type source type} for the transform
   * @param <T> Java type passed to this transform
   * @return a day transform
   */
  @SuppressWarnings("unchecked")
  public static <T> Transform<T, Integer> day(Type type) {
    switch (type.typeId()) {
      case DATE:
        return (Transform<T, Integer>) Dates.DAY;
      case TIMESTAMP:
        return (Transform<T, Integer>) Timestamps.DAY;
      default:
        throw new IllegalArgumentException(
            "Cannot partition type " + type + " by day");
    }
  }

  /**
   * Returns a hour {@link Transform} for timestamps.
   *
   * @param type the {@link Type source type} for the transform
   * @param <T> Java type passed to this transform
   * @return a hour transform
   */
  @SuppressWarnings("unchecked")
  public static <T> Transform<T, Integer> hour(Type type) {
    Preconditions.checkArgument(type.typeId() == Type.TypeID.TIMESTAMP,
        "Cannot partition type %s by hour", type);
    return (Transform<T, Integer>) Timestamps.HOUR;
  }

  /**
   * Returns a bucket {@link Transform} for the given type and number of buckets.
   *
   * @param type the {@link Type source type} for the transform
   * @param numBuckets the number of buckets for the transform to produce
   * @param <T> Java type passed to this transform
   * @return a transform that buckets values into numBuckets
   */
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
   */
  public static <T> Transform<T, T> truncate(Type type, int width) {
    return Truncate.get(type, width);
  }
}
