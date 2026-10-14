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

/** Base type for source state records in a materialized view's refresh state. */
public interface SourceState {

  /** The type discriminator for this source state record. */
  String type();

  /** The name of the source object. */
  String name();

  /** The namespace levels of the source object. */
  List<String> namespace();

  /** The catalog of the source object, or null if the same as the materialized view's catalog. */
  @Nullable
  String catalog();

  /** The UUID of the source object. */
  String uuid();
}
