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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

@Internal
class WriteTarget implements Serializable {
  private final String tableName;
  private final String branch;
  private final Integer schemaId;
  private final Integer specId;
  private final boolean upsertMode;
  private final List<Integer> equalityFields;

  WriteTarget(
      String tableName,
      String branch,
      Integer schemaId,
      Integer specId,
      boolean upsertMode,
      List<Integer> equalityFields) {
    this.tableName = tableName;
    this.branch = branch != null ? branch : "main";
    this.schemaId = schemaId;
    this.specId = specId;
    this.upsertMode = upsertMode;
    this.equalityFields = equalityFields;
  }

  String tableName() {
    return tableName;
  }

  String branch() {
    return branch;
  }

  Integer schemaId() {
    return schemaId;
  }

  Integer specId() {
    return specId;
  }

  boolean upsertMode() {
    return upsertMode;
  }

  List<Integer> equalityFields() {
    return equalityFields;
  }

  void serializeTo(DataOutputView view) throws IOException {
    view.writeUTF(tableName);
    view.writeUTF(branch);
    view.writeInt(schemaId);
    view.writeInt(specId);
    view.writeBoolean(upsertMode);
    view.writeInt(equalityFields.size());
    for (Integer equalityField : equalityFields) {
      view.writeInt(equalityField);
    }
  }

  static WriteTarget deserializeFrom(DataInputView view) throws IOException {
    return new WriteTarget(
        view.readUTF(),
        view.readUTF(),
        view.readInt(),
        view.readInt(),
        view.readBoolean(),
        readList(view));
  }

  private static List<Integer> readList(DataInputView view) throws IOException {
    int numFields = view.readInt();
    List<Integer> equalityFields = Lists.newArrayList();
    for (int i = 0; i < numFields; i++) {
      equalityFields.add(view.readInt());
    }
    return equalityFields;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    WriteTarget that = (WriteTarget) other;
    return Objects.equals(tableName, that.tableName)
        && Objects.equals(branch, that.branch)
        && Objects.equals(schemaId, that.schemaId)
        && Objects.equals(specId, that.specId)
        && upsertMode == that.upsertMode
        && Objects.equals(equalityFields, that.equalityFields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, branch, schemaId, specId, upsertMode, equalityFields);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("tableName", tableName)
        .add("branch", branch)
        .add("schemaId", schemaId)
        .add("specId", specId)
        .add("upsertMode", upsertMode)
        .toString();
  }
}
