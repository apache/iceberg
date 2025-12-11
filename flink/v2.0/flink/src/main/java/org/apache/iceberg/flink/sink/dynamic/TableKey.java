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
import java.util.Objects;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

class TableKey implements Serializable {
  private final String tableName;
  private final String branch;

  TableKey(String tableName, String branch) {
    this.tableName = tableName;
    this.branch = branch;
  }

  TableKey(DynamicCommittable committable) {
    this.tableName = committable.key().tableName();
    this.branch = committable.key().branch();
  }

  String tableName() {
    return tableName;
  }

  String branch() {
    return branch;
  }

  void serializeTo(DataOutputView view) throws IOException {
    view.writeUTF(tableName);
    view.writeUTF(branch);
  }

  static TableKey deserializeFrom(DataInputView view) throws IOException {
    return new TableKey(view.readUTF(), view.readUTF());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    TableKey that = (TableKey) other;
    return tableName.equals(that.tableName) && branch.equals(that.branch);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, branch);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("tableName", tableName)
        .add("branch", branch)
        .toString();
  }
}
