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
import org.apache.iceberg.expressions.PathUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.variants.Variant;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

class TestVariantExtractionPathResolver {

  private static final List<String> VARIANT_PATH = ImmutableList.of("v");

  private final GroupType loginField = shreddedStringField("login");
  private final GroupType userGroup =
      org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
          .optional(PrimitiveType.PrimitiveTypeName.BINARY)
          .named("value")
          .optionalGroup()
          .addFields(loginField)
          .named("typed_value")
          .named("user");
  private final GroupType prGroup =
      org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
          .optional(PrimitiveType.PrimitiveTypeName.BINARY)
          .named("value")
          .optionalGroup()
          .addFields(userGroup, shreddedStringField("created_at"))
          .named("typed_value")
          .named("pull_request");
  private final GroupType shreddedVariant = shreddedObjectVariant("v", prGroup);
  private final GroupType unshreddedVariant = unshreddedVariant("v");

  @Test
  void readerPathInsertsTypedValueAfterVariantColumn() {
    assertThat(
            VariantExtractionPathResolver.readerPath(
                VARIANT_PATH,
                ImmutableList.of(
                    new PathUtil.PathSegment.Name("pull_request"),
                    new PathUtil.PathSegment.Name("user"))))
        .containsExactly("v", "typed_value", "pull_request", "user");
  }

  @Test
  void pathArrayAppendsSuffixParts() {
    assertThat(VariantExtractionPathResolver.pathArray(VARIANT_PATH, "metadata"))
        .containsExactly("v", "metadata");
    assertThat(VariantExtractionPathResolver.pathArray(VARIANT_PATH, "value"))
        .containsExactly("v", "value");
  }

  @Test
  void pathToSerializedFieldAppendsValueAfterObjectPath() {
    assertThat(
            VariantExtractionPathResolver.pathToSerializedField(
                VARIANT_PATH,
                ImmutableList.of(
                    new PathUtil.PathSegment.Name("pull_request"),
                    new PathUtil.PathSegment.Name("user"),
                    new PathUtil.PathSegment.Name("login"))))
        .containsExactly("v", "typed_value", "pull_request", "user", "login", "value");
  }

  @Test
  void resolveShreddedFieldGroupWalksDirectTypedValueChildren() {
    // Each segment must be a direct child of the current typed_value container.
    assertThat(
            VariantExtractionPathResolver.resolveShreddedFieldGroup(
                shreddedVariant, ImmutableList.of(new PathUtil.PathSegment.Name("pull_request"))))
        .isEqualTo(prGroup);

    GroupType flatUserVariant = shreddedObjectVariant("v", userGroup);
    assertThat(
            VariantExtractionPathResolver.resolveShreddedFieldGroup(
                flatUserVariant, ImmutableList.of(new PathUtil.PathSegment.Name("user"))))
        .isEqualTo(userGroup);
  }

  @Test
  void resolveShreddedFieldGroupDoesNotCrossTypedValueBoundaries() {
    // Deeper JSON paths are handled by SelectiveObjectReader; this resolver only walks
    // field names that are direct children of each typed_value group.
    assertThat(
            VariantExtractionPathResolver.resolveShreddedFieldGroup(
                shreddedVariant,
                ImmutableList.of(
                    new PathUtil.PathSegment.Name("pull_request"),
                    new PathUtil.PathSegment.Name("user"))))
        .isNull();
    assertThat(
            VariantExtractionPathResolver.resolveShreddedFieldGroup(
                shreddedVariant,
                ImmutableList.of(
                    new PathUtil.PathSegment.Name("pull_request"),
                    new PathUtil.PathSegment.Name("user"),
                    new PathUtil.PathSegment.Name("login"))))
        .isNull();
  }

  @Test
  void resolveShreddedFieldGroupReturnsTypedValueContainerForEmptyPath() {
    GroupType typedValueGroup = shreddedVariant.getType("typed_value").asGroupType();
    assertThat(VariantExtractionPathResolver.resolveShreddedFieldGroup(shreddedVariant, List.of()))
        .isEqualTo(typedValueGroup);
  }

  @Test
  void resolveShreddedFieldGroupReturnsNullForMissingOrUnshreddedPaths() {
    assertThat(
            VariantExtractionPathResolver.resolveShreddedFieldGroup(
                shreddedVariant, ImmutableList.of(new PathUtil.PathSegment.Name("missing"))))
        .isNull();
    assertThat(
            VariantExtractionPathResolver.resolveShreddedFieldGroup(
                shreddedVariant,
                ImmutableList.of(
                    new PathUtil.PathSegment.Name("pull_request"),
                    new PathUtil.PathSegment.Name("missing"))))
        .isNull();
    assertThat(
            VariantExtractionPathResolver.resolveShreddedFieldGroup(
                unshreddedVariant, ImmutableList.of(new PathUtil.PathSegment.Name("pull_request"))))
        .isNull();
  }

  @Test
  void hasTypedValueAndHasSerializedValueDetectShreddedLayout() {
    assertThat(VariantExtractionPathResolver.hasTypedValue(prGroup)).isTrue();
    assertThat(VariantExtractionPathResolver.hasSerializedValue(prGroup)).isTrue();
    assertThat(VariantExtractionPathResolver.hasTypedValue(loginField)).isTrue();
    assertThat(VariantExtractionPathResolver.hasSerializedValue(loginField)).isTrue();
  }

  @Test
  void hasRootSerializedValueMatchesRootVariantValueColumn() {
    assertThat(VariantExtractionPathResolver.hasRootSerializedValue(shreddedVariant)).isTrue();
    assertThat(VariantExtractionPathResolver.hasRootSerializedValue(unshreddedVariant)).isTrue();
  }

  @Test
  void segmentsBeforeFirstIndexTruncatesAtIndexSegment() {
    assertThat(
            VariantExtractionPathResolver.segmentsBeforeFirstIndex(
                PathUtil.parse("$.items[0].name")))
        .containsExactly(new PathUtil.PathSegment.Name("items"));
    assertThat(
            VariantExtractionPathResolver.segmentsBeforeFirstIndex(PathUtil.parse("$.actor.login")))
        .containsExactly(
            new PathUtil.PathSegment.Name("actor"), new PathUtil.PathSegment.Name("login"));
    assertThat(VariantExtractionPathResolver.segmentsBeforeFirstIndex(PathUtil.parse("$[0]")))
        .isEmpty();
  }

  private static GroupType shreddedObjectVariant(String name, GroupType... objectFields) {
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("metadata")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .optionalGroup()
        .addFields(objectFields)
        .named("typed_value")
        .named(name);
  }

  private static GroupType unshreddedVariant(String name) {
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("metadata")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .named(name);
  }

  private static GroupType shreddedStringField(String name) {
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("typed_value")
        .named(name);
  }
}
