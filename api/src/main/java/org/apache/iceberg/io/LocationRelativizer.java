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
package org.apache.iceberg.io;

import java.io.Serializable;

/**
 * Interface for converting absolute paths to relative and vice-versa.
 *
 * <p>Implementations must be {@link Serializable} because instances will be serialized to tasks.
 */
public interface LocationRelativizer extends Serializable {

  /*

   */
  boolean isRelative();
  /**
   * Return a relative path for the given path w.r.t. provided prefix.
   * The writer code-paths should use this method to relativize path (if required).
   *
   * @param path of the file
   * @return relative path of the input path
   */
  String getRelativePath(String path);

  /**
   * Return absolute path for the given path w.r.t. provided prefix.
   * The reader code-paths should use this method.
   *
   * @param path of the file
   * @return absolute path of the input path
   */
  String getAbsolutePath(String path);
}
