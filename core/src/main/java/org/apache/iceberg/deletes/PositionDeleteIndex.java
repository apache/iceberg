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
package org.apache.iceberg.deletes;

public interface PositionDeleteIndex {
  /**
   * Set a deleted row position.
   *
   * @param position the deleted row position
   */
  void delete(long position);

  /**
   * Set a range of deleted row positions.
   *
   * @param posStart inclusive beginning of position range
   * @param posEnd exclusive ending of position range
   */
  void delete(long posStart, long posEnd);

  /**
   * Checks whether a row at the position is deleted.
   *
   * @param position deleted row position
   * @return whether the position is deleted
   */
  boolean isDeleted(long position);

  /** Returns true if this collection contains no element. */
  boolean isEmpty();
}
