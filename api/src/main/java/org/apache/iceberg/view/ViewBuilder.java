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
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.ViewCatalog;

/**
 * A builder used to create or replace a SQL {@link View}.
 *
 * <p>Call {@link ViewCatalog#buildView} to create a new builder.
 */
public interface ViewBuilder {
  /**
   * Set the view schema.
   *
   * @param schema view schema
   * @return this for method chaining
   */
  ViewBuilder withSchema(Schema schema);

  /**
   * Set the view query.
   *
   * @param query view query
   * @return this for method chaining
   */
  ViewBuilder withQuery(String query);

  /**
   * Set the view SQL dialect.
   *
   * @param dialect view SQL dialect
   * @return this for method chaining
   */
  ViewBuilder withDialect(String dialect);

  /**
   * Set the view default catalog.
   *
   * @param defaultCatalog view default catalog
   * @return this for method chaining
   */
  ViewBuilder withDefaultCatalog(String defaultCatalog);

  /**
   * Set the view default namespace.
   *
   * @param defaultNamespace view default namespace
   * @return this for method chaining
   */
  ViewBuilder withDefaultNamespace(Namespace defaultNamespace);

  /**
   * Set the view query column names.
   *
   * @param queryColumnNames view query column names
   * @return this for method chaining
   */
  ViewBuilder withQueryColumnNames(List<String> queryColumnNames);

  /**
   * Set the view field aliases.
   *
   * @param fieldAliases view field aliases
   * @return this for method chaining
   */
  ViewBuilder withFieldAliases(List<String> fieldAliases);

  /**
   * Set the view field comments.
   *
   * @param fieldComments view field comments
   * @return this for method chaining
   */
  ViewBuilder withFieldComments(List<String> fieldComments);

  /**
   * Add key/value properties to the view.
   *
   * @param properties key/value properties
   * @return this for method chaining
   */
  ViewBuilder withProperties(Map<String, String> properties);

  /**
   * Add a key/value property to the view.
   *
   * @param key a key
   * @param value a value
   * @return this for method chaining
   */
  ViewBuilder withProperty(String key, String value);

  /**
   * Create the view.
   *
   * @return the view created
   */
  View create();

  /**
   * Replace the view.
   *
   * @return the {@link View} replaced
   */
  View replace();

  /**
   * Create or replace the view.
   *
   * @return the {@link View} created or replaced
   */
  View createOrReplace();
}
