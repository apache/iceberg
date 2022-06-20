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
import java.util.Map;

/**
 * Base {@link View} implementation.
 *
 * <p>This can be extended by providing a {@link ViewOperations} to the constructor.
 */
public class BaseView implements View, HasViewOperations {
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

  @Override
  public ViewOperations operations() {
    return ops;
  }

  @Override
  public ViewVersion currentVersion() {
    return ops.current().currentVersion();
  }

  @Override
  public ViewVersion version(int versionId) {
    return ops.current().version(versionId);
  }

  @Override
  public Iterable<ViewVersion> versions() {
    return ops.current().versions();
  }

  @Override
  public List<ViewHistoryEntry> history() {
    return ops.current().history();
  }

  @Override
  public ViewUpdateProperties updateProperties() {
    return new ViewPropertiesUpdate(ops);
  }

  @Override
  public Map<String, String> properties() {
    return ops.current().properties();
  }
}
