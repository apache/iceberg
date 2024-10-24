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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;

// TODO - This was copied from the kafka-connect package almost verbatim, can copy their tests for
// this utility

// TODO - Ask if backwards compatibility is important -- I removed the 'hours','days', (etc)
// transforms

public class PartitionSpecUtil {
  private PartitionSpecUtil() {}

  private static final Pattern TRANSFORM_REGEX = Pattern.compile("(\\w+)\\((.+)\\)");

  public static PartitionSpec createPartitionSpec(
      org.apache.iceberg.Schema schema, String partitionByStatement) {
    if (partitionByStatement.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    List<String> partitionBy = Splitter.on(',').splitToList(partitionByStatement);

    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
    partitionBy.forEach(
        partitionField -> {
          Matcher matcher = TRANSFORM_REGEX.matcher(partitionField);
          if (matcher.matches()) {
            String transform = matcher.group(1);
            switch (transform) {
              case "year":
                specBuilder.year(matcher.group(2));
                break;
              case "month":
                specBuilder.month(matcher.group(2));
                break;
              case "day":
                specBuilder.day(matcher.group(2));
                break;
              case "hour":
                specBuilder.hour(matcher.group(2));
                break;
              case "bucket":
                {
                  Pair<String, Integer> args = transformArgPair(matcher.group(2));
                  specBuilder.bucket(args.first(), args.second());
                  break;
                }
              case "truncate":
                {
                  Pair<String, Integer> args = transformArgPair(matcher.group(2));
                  specBuilder.truncate(args.first(), args.second());
                  break;
                }
              default:
                throw new UnsupportedOperationException("Unsupported transform: " + transform);
            }
          } else {
            specBuilder.identity(partitionField);
          }
        });
    return specBuilder.build();
  }

  private static Pair<String, Integer> transformArgPair(String argsStr) {
    List<String> parts = Splitter.on(',').splitToList(argsStr);
    if (parts.size() != 2) {
      throw new IllegalArgumentException("Invalid argument " + argsStr + ", should have 2 parts");
    }
    return Pair.of(parts.get(0).trim(), Integer.parseInt(parts.get(1).trim()));
  }
}
