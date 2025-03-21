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
package org.apache.iceberg.connect;

import org.apache.iceberg.common.DynConstructors;

class CommitterFactory {
  static Committer createCommitter(IcebergSinkConfig config) {
    String committerImpl = config.committerImpl();
    DynConstructors.Ctor<Committer> ctor;
    try {
      ctor = DynConstructors.builder(Committer.class).impl(committerImpl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Committer implementation %s: %s", committerImpl, e.getMessage()),
          e);
    }

    Committer committer;
    try {
      committer = ctor.newInstance();

    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Committer, %s does not implement Committer.", committerImpl),
          e);
    }
    return committer;
  }

  private CommitterFactory() {}
}
