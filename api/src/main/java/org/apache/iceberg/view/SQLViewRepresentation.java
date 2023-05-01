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
import org.apache.iceberg.catalog.Namespace;
import org.immutables.value.Value;

@Value.Immutable
public interface SQLViewRepresentation extends ViewRepresentation {

  @Override
  default String type() {
    return Type.SQL;
  }

  /** The view query SQL text. */
  String sql();

  /** The view query SQL dialect. */
  String dialect();

  /** The default catalog when the view is created. */
  @Nullable
  String defaultCatalog();

  /** The default namespace when the view is created. */
  @Nullable
  Namespace defaultNamespace();

  /** The view field comments. */
  List<String> fieldComments();

  /** The view field aliases. */
  List<String> fieldAliases();
}
