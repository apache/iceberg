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
import java.util.stream.Collectors;

/**
 * A struct of partition values.
 *
 * <p>Instances of this class can produce partition values from a data row passed to {@link
 * #partition(StructLike)}.
 */
public class PartitionKey extends StructTransform {

  private final PartitionSpec spec;
  private final Schema inputSchema;

  @SuppressWarnings("unchecked")
  public PartitionKey(PartitionSpec spec, Schema inputSchema) {
    super(inputSchema, fieldTransform(spec));
    this.spec = spec;
    this.inputSchema = inputSchema;
  }

  private PartitionKey(PartitionKey toCopy) {
    // only need deep copy inside StructTransform
    super(toCopy);
    this.spec = toCopy.spec;
    this.inputSchema = toCopy.inputSchema;
  }

  public PartitionKey copy() {
    return new PartitionKey(this);
  }

  public String toPath() {
    return spec.partitionToPath(this);
  }

  /**
   * Replace this key's partition values with the partition values for the row.
   *
   * @param row a StructLike row
   */
  @SuppressWarnings("unchecked")
  public void partition(StructLike row) {
    wrap(row);
  }

  private static List<FieldTransform> fieldTransform(PartitionSpec spec) {
    return spec.fields().stream()
        .map(
            partitionField ->
                new FieldTransform(partitionField.sourceIds(), partitionField.transform()))
        .collect(Collectors.toList());
  }
}
