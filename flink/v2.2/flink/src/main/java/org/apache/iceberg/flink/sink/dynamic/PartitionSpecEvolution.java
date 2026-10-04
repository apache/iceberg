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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.List;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** Checks compatibility of PartitionSpecs and evolves one into the other. */
public class PartitionSpecEvolution {

  private PartitionSpecEvolution() {}

  /**
   * Checks whether two PartitionSpecs are compatible with each other. Less strict than {@code
   * PartitionSpec#compatible} in the sense that it tolerates differently named partition fields, as
   * long as their transforms and field names corresponding to their source ids match.
   */
  public static boolean checkCompatibility(PartitionSpec spec1, PartitionSpec spec2) {
    if (spec1.equals(spec2)) {
      return true;
    }

    if (spec1.fields().size() != spec2.fields().size()) {
      return false;
    }

    for (int i = 0; i < spec1.fields().size(); i++) {
      PartitionField field1 = spec1.fields().get(i);
      PartitionField field2 = spec2.fields().get(i);
      if (!specFieldsAreCompatible(field1, spec1.schema(), field2, spec2.schema())) {
        return false;
      }
    }

    return true;
  }

  static PartitionSpecChanges evolve(PartitionSpec currentSpec, PartitionSpec targetSpec) {
    if (currentSpec.compatibleWith(targetSpec)) {
      return new PartitionSpecChanges();
    }

    PartitionSpecChanges result = new PartitionSpecChanges();

    int maxNumFields = Math.max(currentSpec.fields().size(), targetSpec.fields().size());
    for (int i = 0; i < maxNumFields; i++) {
      PartitionField currentField = Iterables.get(currentSpec.fields(), i, null);
      PartitionField targetField = Iterables.get(targetSpec.fields(), i, null);

      if (!specFieldsAreCompatible(
          currentField, currentSpec.schema(), targetField, targetSpec.schema())) {

        if (currentField != null) {
          result.remove(toTerm(currentField, currentSpec.schema()));
        }

        if (targetField != null) {
          result.add(toTerm(targetField, targetSpec.schema()));
        }
      }
    }

    return result;
  }

  static class PartitionSpecChanges {
    private final List<Term> termsToAdd = Lists.newArrayList();
    private final List<Term> termsToRemove = Lists.newArrayList();

    public void add(Term term) {
      termsToAdd.add(term);
    }

    public void remove(Term term) {
      termsToRemove.add(term);
    }

    public List<Term> termsToAdd() {
      return termsToAdd;
    }

    public List<Term> termsToRemove() {
      return termsToRemove;
    }

    public boolean isEmpty() {
      return termsToAdd.isEmpty() && termsToRemove.isEmpty();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(PartitionSpecEvolution.class)
          .add("termsToAdd", termsToAdd)
          .add("termsToRemove", termsToRemove)
          .toString();
    }
  }

  private static Term toTerm(PartitionField field, Schema schema) {
    String sourceName = schema.idToName().get(field.sourceId());
    return Expressions.transform(sourceName, field.transform());
  }

  private static boolean specFieldsAreCompatible(
      PartitionField field1, Schema schemaField1, PartitionField field2, Schema schemaField2) {
    if (field1 == null || field2 == null) {
      return false;
    }
    String firstFieldSourceName = schemaField1.idToName().get(field1.sourceId());
    String secondFieldSourceName = schemaField2.idToName().get(field2.sourceId());
    return firstFieldSourceName.equals(secondFieldSourceName)
        && field1.transform().toString().equals(field2.transform().toString());
  }
}
