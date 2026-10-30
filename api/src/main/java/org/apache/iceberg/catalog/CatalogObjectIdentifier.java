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

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A reference to a catalog object as an ordered list of hierarchical levels (for example, a table,
 * view, or namespace). The kind of object is determined by context — the endpoint or a companion
 * type discriminator — not by the identifier structure alone.
 *
 * <p>Mirrors {@link Namespace} structurally; the distinct name signals "any object within a
 * catalog" and avoids confusion with a future top-level catalog name.
 */
public class CatalogObjectIdentifier {
  private static final Joiner DOT = Joiner.on('.');
  private static final Predicate<String> CONTAINS_NULL_CHARACTER =
      Pattern.compile("\u0000", Pattern.UNICODE_CHARACTER_CLASS).asPredicate();

  public static CatalogObjectIdentifier of(String... levels) {
    return new CatalogObjectIdentifier(levels);
  }

  private final String[] levels;

  private CatalogObjectIdentifier(String[] levels) {
    Preconditions.checkArgument(
        null != levels, "Cannot create catalog object identifier from null array");

    for (String level : levels) {
      Preconditions.checkNotNull(
          level, "Cannot create a catalog object identifier with a null level");
      Preconditions.checkArgument(
          !CONTAINS_NULL_CHARACTER.test(level),
          "Cannot create a catalog object identifier with the null-byte character");
    }

    this.levels = levels;
  }

  public String[] levels() {
    return levels;
  }

  public String level(int pos) {
    return levels[pos];
  }

  public int length() {
    return levels.length;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    CatalogObjectIdentifier that = (CatalogObjectIdentifier) other;
    return Arrays.equals(levels, that.levels);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(levels);
  }

  @Override
  public String toString() {
    return DOT.join(levels);
  }
}
