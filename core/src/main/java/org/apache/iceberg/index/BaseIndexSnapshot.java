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
package org.apache.iceberg.index;

import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * Index snapshot linking an index snapshot to a specific table snapshot.
 *
 * <p>Index data is versioned using snapshots, similar to table data. Each index snapshot is derived
 * from a specific table snapshot, ensuring consistency.
 */
@Value.Immutable
@SuppressWarnings("ImmutablesStyle")
@Value.Style(
    typeImmutable = "ImmutableIndexSnapshot",
    visibilityString = "PUBLIC",
    builderVisibilityString = "PUBLIC")
interface BaseIndexSnapshot extends IndexSnapshot {

  @Override
  @Nullable
  Map<String, String> properties();
}
