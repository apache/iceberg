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
package org.apache.iceberg;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.BoundTerm;
import org.apache.iceberg.expressions.BoundTransform;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.Pair;

class BaseUpdatePartitionSpec implements UpdatePartitionSpec {
  private final TableOperations ops;
  private final TableMetadata base;
  private final int formatVersion;
  private final PartitionSpec spec;
  private final Schema schema;
  private final Map<String, PartitionField> nameToField;
  private final Map<Pair<Integer, String>, PartitionField> transformToField;

  private final List<PartitionField> adds = Lists.newArrayList();
  private final Map<Integer, PartitionField> addedTimeFields = Maps.newHashMap();
  private final Map<Pair<Integer, String>, PartitionField> transformToAddedField =
      Maps.newHashMap();
  private final Map<String, PartitionField> nameToAddedField = Maps.newHashMap();
  private final Set<Object> deletes = Sets.newHashSet();
  private final Map<String, String> renames = Maps.newHashMap();

  private boolean caseSensitive;
  private int lastAssignedPartitionId;

  private final List<Validation> pendingValidations = Lists.newArrayList();

  BaseUpdatePartitionSpec(TableOperations ops) {
    this.ops = ops;
    this.caseSensitive = true;
    this.base = ops.current();
    this.formatVersion = base.formatVersion();
    this.spec = base.spec();
    this.schema = spec.schema();
    this.nameToField = indexSpecByName(spec);
    this.transformToField = indexSpecByTransform(spec);
    this.lastAssignedPartitionId = base.lastAssignedPartitionId();

    spec.fields().stream()
        .filter(field -> field.transform() instanceof UnknownTransform)
        .findAny()
        .ifPresent(
            field -> {
              throw new IllegalArgumentException(
                  "Cannot update partition spec with unknown transform: " + field);
            });
  }

  /** For testing only. */
  @VisibleForTesting
  BaseUpdatePartitionSpec(int formatVersion, PartitionSpec spec) {
    this(formatVersion, spec, spec.lastAssignedFieldId());
  }

  /** For testing only. */
  @VisibleForTesting
  BaseUpdatePartitionSpec(int formatVersion, PartitionSpec spec, int lastAssignedPartitionId) {
    this.ops = null;
    this.base = null;
    this.formatVersion = formatVersion;
    this.caseSensitive = true;
    this.spec = spec;
    this.schema = spec.schema();
    this.nameToField = indexSpecByName(spec);
    this.transformToField = indexSpecByTransform(spec);
    this.lastAssignedPartitionId = lastAssignedPartitionId;
  }

  private int assignFieldId() {
    this.lastAssignedPartitionId += 1;
    return lastAssignedPartitionId;
  }

  /**
   * In V2 it searches for a similar partition field in historical partition specs. Tries to match
   * on source field ID, transform type and target name (optional). If not found or in V1 cases it
   * creates a new PartitionField.
   *
   * @param sourceTransform pair of source ID and transform for this PartitionField addition
   * @param name target partition field name, if specified
   * @return the recycled or newly created partition field
   */
  private PartitionField recycleOrCreatePartitionField(
      Pair<Integer, Transform<?, ?>> sourceTransform, String name) {
    if (formatVersion == 2 && base != null) {
      int sourceId = sourceTransform.first();
      Transform<?, ?> transform = sourceTransform.second();

      Set<PartitionField> allHistoricalFields = Sets.newHashSet();
      for (PartitionSpec partitionSpec : base.specs()) {
        allHistoricalFields.addAll(partitionSpec.fields());
      }

      for (PartitionField field : allHistoricalFields) {
        if (field.sourceId() == sourceId && field.transform().equals(transform)) {
          // if target name is specified then consider it too, otherwise not
          if (name == null || field.name().equals(name)) {
            return field;
          }
        }
      }
    }
    return new PartitionField(
        sourceTransform.first(), assignFieldId(), name, sourceTransform.second());
  }

  @Override
  public UpdatePartitionSpec caseSensitive(boolean isCaseSensitive) {
    this.caseSensitive = isCaseSensitive;
    return this;
  }

  @Override
  public BaseUpdatePartitionSpec addField(String sourceName) {
    return addField(Expressions.ref(sourceName));
  }

  @Override
  public BaseUpdatePartitionSpec addField(Term term) {
    return addField(null, term);
  }

  private BaseUpdatePartitionSpec rewriteDeleteAndAddField(PartitionField existing, String name) {
    deletes.remove(existing.fieldId());
    if (name == null || existing.name().equals(name)) {
      return this;
    } else {
      return renameField(existing.name(), name);
    }
  }

  @Override
  public BaseUpdatePartitionSpec addField(String name, Term term) {
    PartitionField alreadyAdded = nameToAddedField.get(name);
    Preconditions.checkArgument(
        alreadyAdded == null, "Cannot add duplicate partition field: %s", alreadyAdded);

    Pair<Integer, Transform<?, ?>> sourceTransform = resolve(term);
    Pair<Integer, String> validationKey =
        Pair.of(sourceTransform.first(), sourceTransform.second().toString());

    PartitionField existing = transformToField.get(validationKey);
    if (existing != null
        && deletes.contains(existing.fieldId())
        && existing.transform().equals(sourceTransform.second())) {
      return rewriteDeleteAndAddField(existing, name);
    }

    Preconditions.checkArgument(
        existing == null
            || (deletes.contains(existing.fieldId())
                && !existing.transform().toString().equals(sourceTransform.second().toString())),
        "Cannot add duplicate partition field %s=%s, conflicts with %s",
        name,
        term,
        existing);

    PartitionField added = transformToAddedField.get(validationKey);
    Preconditions.checkArgument(
        added == null,
        "Cannot add duplicate partition field %s=%s, already added: %s",
        name,
        term,
        added);

    PartitionField newField = recycleOrCreatePartitionField(sourceTransform, name);
    if (newField.name() == null) {
      String partitionName =
          PartitionSpecVisitor.visit(schema, newField, PartitionNameGenerator.INSTANCE);
      newField =
          new PartitionField(
              newField.sourceId(), newField.fieldId(), partitionName, newField.transform());
    }

    checkForRedundantAddedPartitions(newField);
    transformToAddedField.put(validationKey, newField);

    PartitionField existingField = nameToField.get(newField.name());
    if (existingField != null && !deletes.contains(existingField.fieldId())) {
      if (isVoidTransform(existingField)) {
        // rename the old deleted field that is being replaced by the new field
        renameField(existingField.name(), existingField.name() + "_" + existingField.fieldId());
      } else {
        throw new IllegalArgumentException(
            String.format("Cannot add duplicate partition field name: %s", name));
      }
    } else if (existingField != null && deletes.contains(existingField.fieldId())) {
      renames.put(existingField.name(), existingField.name() + "_" + existingField.fieldId());
    }

    nameToAddedField.put(newField.name(), newField);

    adds.add(newField);

    return this;
  }

  @Override
  public BaseUpdatePartitionSpec removeField(String name) {
    PartitionField alreadyAdded = nameToAddedField.get(name);
    Preconditions.checkArgument(
        alreadyAdded == null, "Cannot delete newly added field: %s", alreadyAdded);

    Preconditions.checkArgument(
        renames.get(name) == null, "Cannot rename and delete partition field: %s", name);

    PartitionField field = nameToField.get(name);
    Preconditions.checkArgument(field != null, "Cannot find partition field to remove: %s", name);

    deletes.add(field.fieldId());

    return this;
  }

  @Override
  public BaseUpdatePartitionSpec removeField(Term term) {
    Pair<Integer, Transform<?, ?>> sourceTransform = resolve(term);
    Pair<Integer, String> key =
        Pair.of(sourceTransform.first(), sourceTransform.second().toString());

    PartitionField added = transformToAddedField.get(key);
    Preconditions.checkArgument(added == null, "Cannot delete newly added field: %s", added);

    PartitionField field = transformToField.get(key);
    Preconditions.checkArgument(field != null, "Cannot find partition field to remove: %s", term);
    Preconditions.checkArgument(
        renames.get(field.name()) == null,
        "Cannot rename and delete partition field: %s",
        field.name());

    deletes.add(field.fieldId());

    return this;
  }

  @Override
  public BaseUpdatePartitionSpec renameField(String name, String newName) {
    PartitionField existingField = nameToField.get(newName);
    if (existingField != null && isVoidTransform(existingField)) {
      // rename the old deleted field that is being replaced by the new field
      renameField(existingField.name(), existingField.name() + "_" + existingField.fieldId());
    }

    PartitionField added = nameToAddedField.get(name);
    Preconditions.checkArgument(
        added == null, "Cannot rename newly added partition field: %s", name);

    PartitionField field = nameToField.get(name);
    Preconditions.checkArgument(field != null, "Cannot find partition field to rename: %s", name);
    Preconditions.checkArgument(
        !deletes.contains(field.fieldId()), "Cannot delete and rename partition field: %s", name);

    renames.put(name, newName);

    return this;
  }

  @Override
  public PartitionSpec apply() {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);

    for (PartitionField field : spec.fields()) {
      if (!deletes.contains(field.fieldId())) {
        String newName = renames.get(field.name());
        if (newName != null) {
          builder.add(field.sourceId(), field.fieldId(), newName, field.transform());
        } else {
          builder.add(field.sourceId(), field.fieldId(), field.name(), field.transform());
        }
      } else if (formatVersion < 2) {
        // field IDs were not required for v1 and were assigned sequentially in each partition spec
        // starting at 1,000.
        // to maintain consistent field ids across partition specs in v1 tables, any partition field
        // that is removed
        // must be replaced with a null transform. null values are always allowed in partition data.
        String newName = renames.get(field.name());
        if (newName != null) {
          builder.add(field.sourceId(), field.fieldId(), newName, Transforms.alwaysNull());
        } else {
          builder.add(field.sourceId(), field.fieldId(), field.name(), Transforms.alwaysNull());
        }
      }
    }

    for (PartitionField newField : adds) {
      builder.add(newField.sourceId(), newField.fieldId(), newField.name(), newField.transform());
    }

    return builder.build();
  }

  @Override
  public void validate(List<Validation> validations) {
    ValidationUtils.validate(base, validations);
    pendingValidations.addAll(validations);
  }

  @Override
  public void commit() {
    TableMetadata update = base.updatePartitionSpec(apply());
    ValidationUtils.validate(base, pendingValidations);
    ops.commit(base, update);
  }

  private Pair<Integer, Transform<?, ?>> resolve(Term term) {
    Preconditions.checkArgument(term instanceof UnboundTerm, "Term must be unbound");

    BoundTerm<?> boundTerm = ((UnboundTerm<?>) term).bind(schema.asStruct(), caseSensitive);
    int sourceId = boundTerm.ref().fieldId();
    Transform<?, ?> transform = toTransform(boundTerm);

    Type fieldType = schema.findType(sourceId);
    if (fieldType != null) {
      transform = Transforms.fromString(fieldType, transform.toString());
    } else {
      transform = Transforms.fromString(transform.toString());
    }
    return Pair.of(sourceId, transform);
  }

  private Transform<?, ?> toTransform(BoundTerm<?> term) {
    if (term instanceof BoundReference) {
      return Transforms.identity();
    } else if (term instanceof BoundTransform) {
      return ((BoundTransform<?, ?>) term).transform();
    } else {
      throw new ValidationException(
          "Invalid term: %s, expected either a bound reference or transform", term);
    }
  }

  private void checkForRedundantAddedPartitions(PartitionField field) {
    if (isTimeTransform(field)) {
      PartitionField timeField = addedTimeFields.get(field.sourceId());
      Preconditions.checkArgument(
          timeField == null,
          "Cannot add redundant partition field: %s conflicts with %s",
          timeField,
          field);
      addedTimeFields.put(field.sourceId(), field);
    }
  }

  private static Map<String, PartitionField> indexSpecByName(PartitionSpec spec) {
    ImmutableMap.Builder<String, PartitionField> builder = ImmutableMap.builder();
    List<PartitionField> fields = spec.fields();
    for (PartitionField field : fields) {
      builder.put(field.name(), field);
    }

    return builder.build();
  }

  private static Map<Pair<Integer, String>, PartitionField> indexSpecByTransform(
      PartitionSpec spec) {
    Map<Pair<Integer, String>, PartitionField> indexSpecs = Maps.newHashMap();
    List<PartitionField> fields = spec.fields();
    for (PartitionField field : fields) {
      indexSpecs.put(Pair.of(field.sourceId(), field.transform().toString()), field);
    }

    return indexSpecs;
  }

  private boolean isTimeTransform(PartitionField field) {
    return PartitionSpecVisitor.visit(schema, field, IsTimeTransform.INSTANCE);
  }

  private static class IsTimeTransform implements PartitionSpecVisitor<Boolean> {
    private static final IsTimeTransform INSTANCE = new IsTimeTransform();

    private IsTimeTransform() {}

    @Override
    public Boolean identity(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean bucket(int fieldId, String sourceName, int sourceId, int numBuckets) {
      return false;
    }

    @Override
    public Boolean truncate(int fieldId, String sourceName, int sourceId, int width) {
      return false;
    }

    @Override
    public Boolean year(int fieldId, String sourceName, int sourceId) {
      return true;
    }

    @Override
    public Boolean month(int fieldId, String sourceName, int sourceId) {
      return true;
    }

    @Override
    public Boolean day(int fieldId, String sourceName, int sourceId) {
      return true;
    }

    @Override
    public Boolean hour(int fieldId, String sourceName, int sourceId) {
      return true;
    }

    @Override
    public Boolean alwaysNull(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean unknown(int fieldId, String sourceName, int sourceId, String transform) {
      return false;
    }
  }

  private boolean isVoidTransform(PartitionField field) {
    return PartitionSpecVisitor.visit(schema, field, IsVoidTransform.INSTANCE);
  }

  private static class IsVoidTransform implements PartitionSpecVisitor<Boolean> {
    private static final IsVoidTransform INSTANCE = new IsVoidTransform();

    private IsVoidTransform() {}

    @Override
    public Boolean identity(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean bucket(int fieldId, String sourceName, int sourceId, int numBuckets) {
      return false;
    }

    @Override
    public Boolean truncate(int fieldId, String sourceName, int sourceId, int width) {
      return false;
    }

    @Override
    public Boolean year(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean month(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean day(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean hour(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean alwaysNull(int fieldId, String sourceName, int sourceId) {
      return true;
    }

    @Override
    public Boolean unknown(int fieldId, String sourceName, int sourceId, String transform) {
      return false;
    }
  }

  private static class PartitionNameGenerator implements PartitionSpecVisitor<String> {
    private static final PartitionNameGenerator INSTANCE = new PartitionNameGenerator();

    private PartitionNameGenerator() {}

    @Override
    public String identity(int fieldId, String sourceName, int sourceId) {
      return sourceName;
    }

    @Override
    public String bucket(int fieldId, String sourceName, int sourceId, int numBuckets) {
      return sourceName + "_bucket_" + numBuckets;
    }

    @Override
    public String truncate(int fieldId, String sourceName, int sourceId, int width) {
      return sourceName + "_trunc_" + width;
    }

    @Override
    public String year(int fieldId, String sourceName, int sourceId) {
      return sourceName + "_year";
    }

    @Override
    public String month(int fieldId, String sourceName, int sourceId) {
      return sourceName + "_month";
    }

    @Override
    public String day(int fieldId, String sourceName, int sourceId) {
      return sourceName + "_day";
    }

    @Override
    public String hour(int fieldId, String sourceName, int sourceId) {
      return sourceName + "_hour";
    }

    @Override
    public String alwaysNull(int fieldId, String sourceName, int sourceId) {
      return sourceName + "_null";
    }
  }
}
