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
 * Reference to a named object in a {@link Catalog}, such as {@link Namespace}, {@link
 * org.apache.iceberg.Table}, or {@link org.apache.iceberg.view.View}.
 */
public class CatalogObject {
  private static final CatalogObject EMPTY_CATALOG_OBJECT = new CatalogObject(new String[] {});
  private static final Joiner DOT = Joiner.on(".");
  private static final Predicate<String> CONTAINS_NULL_CHARACTER =
      Pattern.compile("\u0000", Pattern.UNICODE_CHARACTER_CLASS).asPredicate();

  public static CatalogObject empty() {
    return EMPTY_CATALOG_OBJECT;
  }

  public static CatalogObject of(String... levels) {
    Preconditions.checkArgument(null != levels, "Cannot create CatalogObject from null array");
    if (levels.length == 0) {
      return empty();
    }

    for (String level : levels) {
      Preconditions.checkNotNull(level, "Cannot create a CatalogObject with a null level");
      Preconditions.checkArgument(
          !CONTAINS_NULL_CHARACTER.test(level),
          "Cannot create a CatalogObject with the null-byte character");
    }

    return new CatalogObject(levels);
  }

  public static CatalogObject of(String name) {
    Preconditions.checkNotNull(name, "Cannot create CatalogObject from null name");

    return of(name.split("\\."));
  }

  private final String[] levels;

  private CatalogObject(String[] levels) {
    this.levels = levels;
  }

  public String[] levels() {
    return levels;
  }

  public String level(int pos) {
    return levels[pos];
  }

  public boolean isEmpty() {
    return levels.length == 0;
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

    CatalogObject catalogObject = (CatalogObject) other;
    return Arrays.equals(levels, catalogObject.levels);
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
