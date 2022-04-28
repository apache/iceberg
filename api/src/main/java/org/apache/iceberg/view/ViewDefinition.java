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

public interface ViewDefinition extends ViewRepresentation {
  @Override
  default Type type() {
    return Type.SQL;
  }

  /**
   * Returns the view query SQL text.
   *
   * @return the view query SQL text
   */
  String sql();

  /**
   * Returns the SQL dialect of the query SQL text.
   *
   * @return the SQL dialect of the query SQL text
   */
  String dialect();

  /**
   * Returns the view query output schema.
   *
   * @return the view query output schema
   */
  Schema schema();

  /**
   * Returns the default catalog when the view is created.
   *
   * @return the default catalog
   */
  String defaultCatalog();

  /**
   * Returns the default namespace when the view is created.
   *
   * @return the default namespace
   */
  List<String> defaultNamespace();

  /**
   * Returns the field aliases specified when creating the view.
   *
   * @return the field aliases
   */
  List<String> fieldAliases();

  /**
   * Returns the field comments specified when creating the view.
   *
   * @return the field comments
   */
  List<String> fieldComments();
}
