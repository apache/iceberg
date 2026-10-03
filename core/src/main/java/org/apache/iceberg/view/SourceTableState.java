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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Captures the state of a source table at the time of a materialized view refresh. */
public class SourceTableState implements SourceState {
  public static final String TYPE = "table";

  private final String name;
  private final List<String> namespace;
  private final String catalog;
  private final String uuid;
  private final long snapshotId;
  private final String ref;

  public SourceTableState(
      String name,
      List<String> namespace,
      @Nullable String catalog,
      String uuid,
      long snapshotId,
      @Nullable String ref) {
    Preconditions.checkArgument(name != null, "Source table name is required");
    Preconditions.checkArgument(
        namespace != null && !namespace.isEmpty(), "Source table namespace is required");
    Preconditions.checkArgument(uuid != null, "Source table uuid is required");
    this.name = name;
    this.namespace = namespace;
    this.catalog = catalog;
    this.uuid = uuid;
    this.snapshotId = snapshotId;
    this.ref = ref;
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public List<String> namespace() {
    return namespace;
  }

  @Override
  @Nullable
  public String catalog() {
    return catalog;
  }

  @Override
  public String uuid() {
    return uuid;
  }

  public long snapshotId() {
    return snapshotId;
  }

  @Nullable
  public String ref() {
    return ref;
  }
}
