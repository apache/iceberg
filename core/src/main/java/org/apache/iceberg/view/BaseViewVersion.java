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

import javax.annotation.Nullable;
import org.apache.iceberg.catalog.Namespace;
import org.immutables.value.Value;

/**
 * A version of the view at a point in time.
 *
 * <p>A version consists of a view metadata file.
 *
 * <p>Versions are created by view operations, like Create and Replace.
 */
@Value.Immutable
// https://github.com/immutables/immutables/issues/291 does not apply here because we're not adding
// any Immutable-specific class to the classpath
@SuppressWarnings("ImmutablesStyle")
@Value.Style(typeImmutable = "ImmutableViewVersion")
public interface BaseViewVersion extends ViewVersion {

  @Override
  @Value.Lazy
  default String operation() {
    return summary().get("operation");
  }

  @Override
  @Nullable
  String defaultCatalog();

  @Override
  @Nullable
  Namespace defaultNamespace();

  @Override
  @Value.Check
  default void check() {
    ViewVersion.super.check();
  }
}
