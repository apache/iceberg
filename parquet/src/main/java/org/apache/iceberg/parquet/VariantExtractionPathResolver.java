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

import java.util.List;
import org.apache.iceberg.expressions.PathUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

/** Resolves shredded Parquet paths for variant extraction pushdown. */
class VariantExtractionPathResolver {
  private static final String VALUE = "value";
  private static final String TYPED_VALUE = "typed_value";

  private VariantExtractionPathResolver() {}

  static List<String> readerPath(
      Iterable<String> variantColumnPath, List<PathUtil.PathSegment> objectPathParts) {
    List<String> path = Lists.newArrayList(variantColumnPath);
    path.add(TYPED_VALUE);
    for (PathUtil.PathSegment seg : objectPathParts) {
      path.add(((PathUtil.PathSegment.Name) seg).name());
    }
    return path;
  }

  static String[] pathArray(Iterable<String> variantColumnPath, String... suffix) {
    List<String> path = Lists.newArrayList(variantColumnPath);
    for (String part : suffix) {
      path.add(part);
    }
    return path.toArray(new String[0]);
  }

  static GroupType resolveShreddedFieldGroup(
      GroupType variantGroup, List<PathUtil.PathSegment> objectPathParts) {
    if (!ParquetSchemaUtil.hasField(variantGroup, TYPED_VALUE)) {
      return null;
    }

    Type typedValue = variantGroup.getType(TYPED_VALUE);
    if (typedValue.isPrimitive()) {
      return objectPathParts.isEmpty() ? variantGroup : null;
    }

    GroupType current = typedValue.asGroupType();
    for (PathUtil.PathSegment seg : objectPathParts) {
      if (seg instanceof PathUtil.PathSegment.Index) {
        // Array indexes are not shredded as Parquet fields.
        return null;
      }
      String part = ((PathUtil.PathSegment.Name) seg).name();
      if (!ParquetSchemaUtil.hasField(current, part)) {
        return null;
      }

      Type field = current.getType(part);
      if (field.isPrimitive()) {
        return null;
      }

      current = field.asGroupType();
    }

    return current;
  }

  static boolean hasTypedValue(GroupType valueGroup) {
    return ParquetSchemaUtil.hasField(valueGroup, TYPED_VALUE);
  }

  /**
   * Returns true when the given group (either a per-field shredded value group or the root variant
   * group) has an inline serialized {@code value} column. Both call sites check the same predicate;
   * the two names distinguish the caller context for clarity.
   */
  static boolean hasSerializedValue(GroupType valueGroup) {
    return ParquetSchemaUtil.hasField(valueGroup, VALUE);
  }

  /** Returns true when the root variant group has an inline serialized {@code value} column. */
  static boolean hasRootSerializedValue(GroupType variantGroup) {
    return hasSerializedValue(variantGroup);
  }

  static String[] pathToSerializedField(
      List<String> variantColumnPath, List<PathUtil.PathSegment> objectPathParts) {
    List<String> path = Lists.newArrayList(variantColumnPath);
    path.add(TYPED_VALUE);
    for (PathUtil.PathSegment seg : objectPathParts) {
      path.add(((PathUtil.PathSegment.Name) seg).name());
    }
    path.add(VALUE);
    return path.toArray(new String[0]);
  }

  /**
   * Returns the leading segments up to (but not including) the first {@link
   * PathUtil.PathSegment.Index}. For a purely object path such as {@code [Name("actor"),
   * Name("login")]} this is the full list.
   */
  static List<PathUtil.PathSegment> segmentsBeforeFirstIndex(List<PathUtil.PathSegment> segments) {
    for (int i = 0; i < segments.size(); i++) {
      if (segments.get(i) instanceof PathUtil.PathSegment.Index) {
        return segments.subList(0, i);
      }
    }
    return segments;
  }
}
