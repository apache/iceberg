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

import java.util.Map;
import org.apache.iceberg.catalog.ViewCatalog;

/**
 * A builder used to create or replace a SQL {@link View}.
 *
 * <p>Call {@link ViewCatalog#buildView} to create a new builder.
 */
public interface ViewBuilder extends VersionBuilder<ViewBuilder> {

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
   * Sets a location for the view
   *
   * @param location the location to set for the view
   * @return this for method chaining
   */
  default ViewBuilder withLocation(String location) {
    throw new UnsupportedOperationException("Setting a view's location is not supported");
  }

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
