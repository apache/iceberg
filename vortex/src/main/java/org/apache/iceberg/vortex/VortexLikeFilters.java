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
package org.apache.iceberg.vortex;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * String match predicates that Iceberg expressions cannot state, such as a substring or a suffix
 * match, for the Vortex reader to apply in its scan.
 *
 * <p>The engine still evaluates these predicates on every row it receives, so the reader may treat
 * them as a hint: it applies a predicate when the file has the column and skips it otherwise. The
 * filter only removes rows that fail the predicate, which lets Vortex skip chunks and read the
 * other columns for the matching rows only.
 */
public final class VortexLikeFilters {
  /** The read property that carries the encoded predicates, see {@link #encode(List)}. */
  public static final String PROPERTY = "read.vortex.like-filters";

  private static final char ESCAPE = '\\';
  private static final String ENTRY_SEPARATOR = ";";
  private static final String PART_SEPARATOR = ":";

  private VortexLikeFilters() {}

  /** A LIKE match of the string column with the Iceberg field id, negated when requested. */
  public record Like(int fieldId, String pattern, boolean negated) implements Serializable {}

  /** A predicate that matches values that contain {@code value}. */
  public static Like contains(int fieldId, String value, boolean negated) {
    return new Like(fieldId, "%" + escape(value) + "%", negated);
  }

  /** A predicate that matches values that end with {@code value}. */
  public static Like endsWith(int fieldId, String value, boolean negated) {
    return new Like(fieldId, "%" + escape(value), negated);
  }

  public static String encode(List<Like> likes) {
    return likes.stream()
        .map(
            like ->
                like.fieldId()
                    + PART_SEPARATOR
                    + (like.negated() ? 1 : 0)
                    + PART_SEPARATOR
                    + Base64.getEncoder()
                        .encodeToString(like.pattern().getBytes(StandardCharsets.UTF_8)))
        .collect(Collectors.joining(ENTRY_SEPARATOR));
  }

  static List<Like> decode(String encoded) {
    ImmutableList.Builder<Like> likes = ImmutableList.builder();
    for (String entry : Splitter.on(ENTRY_SEPARATOR).omitEmptyStrings().split(encoded)) {
      List<String> parts = Splitter.on(PART_SEPARATOR).splitToList(entry);
      Preconditions.checkArgument(parts.size() == 3, "Invalid Vortex LIKE filter: %s", entry);

      String pattern = new String(Base64.getDecoder().decode(parts.get(2)), StandardCharsets.UTF_8);
      likes.add(new Like(Integer.parseInt(parts.get(0)), pattern, "1".equals(parts.get(1))));
    }

    return likes.build();
  }

  /**
   * Escapes the LIKE wildcards in {@code value}. Vortex LIKE treats {@code %} and {@code _} as
   * wildcards and {@code \} as the escape character.
   */
  private static String escape(String value) {
    StringBuilder escaped = new StringBuilder(value.length() + 4);
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      if (character == ESCAPE || character == '%' || character == '_') {
        escaped.append(ESCAPE);
      }

      escaped.append(character);
    }

    return escaped.toString();
  }
}
