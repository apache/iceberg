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

package org.apache.iceberg.mr.hive;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Parser for inputs received from config {@link org.apache.iceberg.mr.InputFormatConfig#PARTITIONING}
 */
class HiveIcebergPartitionTextParser {
  private static final int PARTITION_TEXT_MAX_LENGTH = 1000;

  private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket\\((.*?),(.*?)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern TRUNCATE_PATTERN = Pattern.compile(
      "truncate\\((.*?),(.*?)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern YEAR_PATTERN = Pattern.compile("year\\((.*?)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern MONTH_PATTERN = Pattern.compile("month\\((.*?)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern DAY_PATTERN = Pattern.compile("day\\((.*?)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern HOUR_PATTERN = Pattern.compile("hour\\((.*?)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern ALWAYS_NULL_PATTERN = Pattern.compile(
      "alwaysNull\\((.*?)\\)", Pattern.CASE_INSENSITIVE);
  private static final String INTEGER_PATTERN = "\\d+";

  private HiveIcebergPartitionTextParser() {
  }

  private static boolean build2ArgTransform(Pattern transformPattern, BiConsumer<String, Integer> transformBuilder,
                                            String trimmedPart, int intArgPos) {
    Matcher matcher = transformPattern.matcher(trimmedPart);
    if (matcher.find()) {
      Preconditions.checkArgument(matcher.groupCount() == 2,
          "Cannot parse 2-arg partition transform from text part: %s", trimmedPart);

      String intArg = matcher.group(intArgPos).trim();
      Preconditions.checkArgument(intArg.matches(INTEGER_PATTERN),
          "Cannot find integer as argument %s in 2-arg partition transform: %s", intArgPos, trimmedPart);
      int transformArg = Integer.parseInt(matcher.group(intArgPos).trim());

      String columnName = matcher.group(intArgPos % 2 + 1).trim();
      transformBuilder.accept(columnName, transformArg);
      return true;
    }

    return false;
  }

  private static boolean build1ArgTransform(Pattern transformPattern, Consumer<String> transformBuilder,
                                            String trimmedPart) {
    Matcher matcher = transformPattern.matcher(trimmedPart);
    if (matcher.find()) {
      Preconditions.checkArgument(matcher.groupCount() == 1,
          "Cannot parse 1-arg partition transform from text part: %s", trimmedPart);
      String columnName = matcher.group(1).trim();
      transformBuilder.accept(columnName);
      return true;
    }

    return false;
  }

  public static PartitionSpec fromText(Schema schema, String text, String delimiter) {
    Preconditions.checkArgument(text.length() < PARTITION_TEXT_MAX_LENGTH,
        "Partition spec text too long: max allowed length %s, but got %s",
        PARTITION_TEXT_MAX_LENGTH, text.length());
    String[] parts = text.split(delimiter);
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);

    for (String part : parts) {
      String trimmedPart = part.trim();
      boolean matched = build2ArgTransform(BUCKET_PATTERN, builder::bucket, trimmedPart, 1) ||
          build2ArgTransform(TRUNCATE_PATTERN, builder::truncate, trimmedPart, 2) ||
          build1ArgTransform(YEAR_PATTERN, builder::year, trimmedPart) ||
          build1ArgTransform(MONTH_PATTERN, builder::month, trimmedPart) ||
          build1ArgTransform(DAY_PATTERN, builder::day, trimmedPart) ||
          build1ArgTransform(HOUR_PATTERN, builder::hour, trimmedPart) ||
          build1ArgTransform(ALWAYS_NULL_PATTERN, builder::alwaysNull, trimmedPart);

      if (!matched) {
        Preconditions.checkArgument(schema.findField(trimmedPart) != null,
            "Cannot recognized partition transform or find column %s in schema %s", trimmedPart, schema);
        builder.identity(trimmedPart);
      }
    }

    return builder.build();
  }
}
