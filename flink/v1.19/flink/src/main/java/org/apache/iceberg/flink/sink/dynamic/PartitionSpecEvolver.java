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
import java.util.Objects;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.expressions.UnboundTransform;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** Checks compatibility of PartitionSpecs and evolves one into the other. */
public class PartitionSpecEvolver {

  private PartitionSpecEvolver() {}

  /**
   * Checks whether two PartitionSpecs are compatible with each other. Less strict than {@code
   * PartitionSpec#compatible} in the sense that it tolerates differently named partition fields, as
   * long as their transforms and source ids match.
   */
  public static boolean checkCompatibility(PartitionSpec first, PartitionSpec second) {
    if (first.equals(second)) {
      return true;
    }

    if (first.fields().size() != second.fields().size()) {
      return false;
    }

    for (int i = 0; i < first.fields().size(); i++) {
      PartitionField firstField = first.fields().get(i);
      PartitionField secondField = second.fields().get(i);
      if (firstField.sourceId() != secondField.sourceId()
          || !firstField.transform().toString().equals(secondField.transform().toString())) {
        return false;
      }
    }

    return true;
  }

  public static PartitionSpecEvolverResult evolve(
      PartitionSpec currentSpec, PartitionSpec targetSpec, Schema schema) {
    if (currentSpec.compatibleWith(targetSpec)) {
      return new PartitionSpecEvolverResult();
    }

    PartitionSpecEvolverResult result = new PartitionSpecEvolverResult();

    int maxNumFields = Math.max(currentSpec.fields().size(), targetSpec.fields().size());
    for (int i = 0; i < maxNumFields; i++) {
      PartitionField currentField = Iterables.get(currentSpec.fields(), i, null);
      PartitionField targetField = Iterables.get(targetSpec.fields(), i, null);

      if (!Objects.equals(currentField, targetField)) {
        if (currentField != null) {
          result.remove(toTerm(currentField, schema));
        }

        if (targetField != null) {
          result.add(toTerm(targetField, schema));
        }
      }
    }

    return result;
  }

  public static class PartitionSpecEvolverResult {
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
  }

  private static Term toTerm(PartitionField field, Schema schema) {
    String sourceName = schema.findField(field.sourceId()).name();
    return new UnboundTransform<>(new NamedReference<>(sourceName), field.transform());
  }
}
