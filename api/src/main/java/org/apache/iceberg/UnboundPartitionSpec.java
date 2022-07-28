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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class UnboundPartitionSpec {

  private final int specId;
  private final List<UnboundPartitionField> fields;

  public UnboundPartitionSpec(int specId, List<UnboundPartitionField> fields) {
    this.specId = specId;
    this.fields = fields;
  }

  public int specId() {
    return specId;
  }

  public List<UnboundPartitionField> fields() {
    return fields;
  }

  public PartitionSpec bind(Schema schema) {
    return copyToBuilder(schema).build();
  }

  PartitionSpec bindUnchecked(Schema schema) {
    return copyToBuilder(schema).buildUnchecked();
  }

  private PartitionSpec.Builder copyToBuilder(Schema schema) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema).withSpecId(specId);

    for (UnboundPartitionField field : fields) {
      if (field.partitionId != null) {
        builder.add(field.sourceId, field.partitionId, field.name, field.transformAsString);
      } else {
        builder.add(field.sourceId, field.name, field.transformAsString);
      }
    }

    return builder;
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder {
    private final List<UnboundPartitionField> fields;
    private int specId = 0;

    private Builder() {
      this.fields = Lists.newArrayList();
    }

    Builder withSpecId(int newSpecId) {
      this.specId = newSpecId;
      return this;
    }

    Builder addField(String transformAsString, int sourceId, int partitionId, String name) {
      fields.add(new UnboundPartitionField(transformAsString, sourceId, partitionId, name));
      return this;
    }

    Builder addField(String transformAsString, int sourceId, String name) {
      fields.add(new UnboundPartitionField(transformAsString, sourceId, null, name));
      return this;
    }

    UnboundPartitionSpec build() {
      return new UnboundPartitionSpec(specId, fields);
    }
  }

  static class UnboundPartitionField {
    private final String transformAsString;
    private final int sourceId;
    private final Integer partitionId;
    private final String name;

    public String transformAsString() {
      return transformAsString;
    }

    public int sourceId() {
      return sourceId;
    }

    public Integer partitionId() {
      return partitionId;
    }

    public String name() {
      return name;
    }

    private UnboundPartitionField(
        String transformAsString, int sourceId, Integer partitionId, String name) {
      this.transformAsString = transformAsString;
      this.sourceId = sourceId;
      this.partitionId = partitionId;
      this.name = name;
    }
  }
}
