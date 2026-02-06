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
package org.apache.iceberg.actions;

import java.util.Map;

/**
 * An action performed on a table.
 *
 * @param <ThisT> the child Java API class, returned by method chaining
 * @param <R> the Java type of the result produced by this action
 */
public interface Action<ThisT, R> {
  /**
   * Configures this action with an extra option.
   *
   * <p>Certain actions allow users to control internal details of their execution via options.
   *
   * @param name an option name
   * @param value an option value
   * @return this for method chaining
   */
  default ThisT option(String name, String value) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement option");
  }

  /**
   * Configures this action with extra options.
   *
   * <p>Certain actions allow users to control internal details of their execution via options.
   *
   * @param options a map of extra options
   * @return this for method chaining
   */
  default ThisT options(Map<String, String> options) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement options");
  }

  /**
   * Executes this action.
   *
   * @return the result of this action
   */
  R execute();
}
