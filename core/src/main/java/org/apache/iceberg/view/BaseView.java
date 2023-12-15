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
package org.apache.iceberg.view;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class BaseView implements View, Serializable {

  private final ViewOperations ops;
  private final String name;

  public BaseView(ViewOperations ops, String name) {
    this.ops = ops;
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  public ViewOperations operations() {
    return ops;
  }

  @Override
  public Schema schema() {
    return operations().current().schema();
  }

  @Override
  public Map<Integer, Schema> schemas() {
    return operations().current().schemasById();
  }

  @Override
  public ViewVersion currentVersion() {
    return operations().current().currentVersion();
  }

  @Override
  public Iterable<ViewVersion> versions() {
    return operations().current().versions();
  }

  @Override
  public ViewVersion version(int versionId) {
    return operations().current().version(versionId);
  }

  @Override
  public List<ViewHistoryEntry> history() {
    return operations().current().history();
  }

  @Override
  public Map<String, String> properties() {
    return operations().current().properties();
  }

  @Override
  public String location() {
    return operations().current().location();
  }

  @Override
  public UpdateViewProperties updateProperties() {
    return new PropertiesUpdate(ops);
  }

  @Override
  public ReplaceViewVersion replaceVersion() {
    return new ViewVersionReplace(ops);
  }

  @Override
  public UpdateLocation updateLocation() {
    return new SetViewLocation(ops);
  }

  @Override
  public UUID uuid() {
    return UUID.fromString(ops.current().uuid());
  }

  /**
   * This implementation of sqlFor will resolve what is considered the "closest" dialect. If an
   * exact match is found, then that is returned. Otherwise, the first representation would be
   * returned. If no SQL representation is found, null is returned.
   */
  @Override
  public SQLViewRepresentation sqlFor(String dialect) {
    Preconditions.checkArgument(dialect != null, "Invalid dialect: null");
    Preconditions.checkArgument(!dialect.isEmpty(), "Invalid dialect: (empty string)");
    SQLViewRepresentation closest = null;
    for (ViewRepresentation representation : currentVersion().representations()) {
      if (representation instanceof SQLViewRepresentation) {
        SQLViewRepresentation sqlViewRepresentation = (SQLViewRepresentation) representation;
        if (sqlViewRepresentation.dialect().equals(dialect)) {
          return sqlViewRepresentation;
        } else if (closest == null) {
          closest = sqlViewRepresentation;
        }
      }
    }

    return closest;
  }
}
