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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

/** Tests for {@code ParquetVariantUti]} */
public class TestParquetVariantUtil {

  @ParameterizedTest
  @FieldSource("PATH_MAPPINGS")
  public void testParsePathSegments(Parsing args) {
    assertThat(ParquetVariantUtil.parsePathSegments(args.input))
        .describedAs("Parsing of path segments %s", args.input)
        .containsExactly(args.output);
  }

  /**
   * A variant path parses to a seqence of column names.
   *
   * @param input input string
   * @param output varargs list of result
   */
  private record Parsing(String input, String... output) {}

  private static final List<Parsing> PATH_MAPPINGS =
      List.of(
          new Parsing("$"),
          new Parsing("$['a']['b']", "a", "b"),
          new Parsing("$['0']['1']", "0", "1"),
          new Parsing("$['user']['firstName']", "user", "firstName"),
          new Parsing("$['_under_score']", "_under_score"));
}
