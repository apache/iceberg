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
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;

public interface SQLViewRepresentation extends ViewRepresentation {

  @Override
  default Type type() {
    return Type.SQL;
  }

  /** The view query SQL text. */
  String query();

  /** The view query SQL dialect. */
  String dialect();

  /** The default catalog when the view is created. */
  String defaultCatalog();

  /** The default namespace when the view is created. */
  Namespace defaultNamespace();

  /** The query output schema at version create time, without aliases. */
  Schema schema();

  /** The view field aliases. */
  List<String> fieldComments();

  /** The view field comments. */
  List<String> fieldAliases();
}
