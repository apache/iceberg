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
package org.apache.iceberg.catalog;

import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.immutables.value.Value;

/** A namespace in a {@link Catalog}. */
@Value.Immutable
public abstract class Namespace {
  private static final Joiner DOT = Joiner.on('.');
  private static final Predicate<String> CONTAINS_NULL_CHARACTER =
      Pattern.compile("\u0000", Pattern.UNICODE_CHARACTER_CLASS).asPredicate();

  public static Namespace empty() {
    return ImmutableNamespace.builder().levels(new String[] {}).build();
  }

  public static Namespace of(String... levels) {
    Preconditions.checkArgument(null != levels, "Cannot create Namespace from null array");
    if (levels.length == 0) {
      return empty();
    }

    for (String level : levels) {
      Preconditions.checkNotNull(level, "Cannot create a namespace with a null level");
      Preconditions.checkArgument(
          !CONTAINS_NULL_CHARACTER.test(level),
          "Cannot create a namespace with the null-byte character");
    }

    return ImmutableNamespace.builder().levels(levels).build();
  }

  public abstract String[] levels();

  public String level(int pos) {
    return levels()[pos];
  }

  public boolean isEmpty() {
    return levels().length == 0;
  }

  public int length() {
    return levels().length;
  }

  @Override
  public String toString() {
    return DOT.join(levels());
  }
}
