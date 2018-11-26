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

package com.netflix.iceberg;

import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import java.util.Collection;
import java.util.Iterator;

public class FilteredSnapshot implements Filterable<FilteredSnapshot> {
  private final SnapshotIterable snapshot;
  private final Expression partFilter;
  private final Expression rowFilter;
  private final Collection<String> columns;

  FilteredSnapshot(SnapshotIterable snapshot,
                   Expression partFilter,
                   Expression rowFilter,
                   Collection<String> columns) {
    this.snapshot = snapshot;
    this.partFilter = partFilter;
    this.rowFilter = rowFilter;
    this.columns = columns;
  }

  @Override
  public FilteredSnapshot select(Collection<String> columns) {
    return new FilteredSnapshot(snapshot, partFilter, rowFilter, columns);
  }

  @Override
  public FilteredSnapshot filterPartitions(Expression expr) {
    return new FilteredSnapshot(snapshot, Expressions.and(partFilter, expr), rowFilter, columns);
  }

  @Override
  public FilteredSnapshot filterRows(Expression expr) {
    return new FilteredSnapshot(snapshot, partFilter, Expressions.and(rowFilter, expr), columns);
  }

  @Override
  public Iterator<DataFile> iterator() {
    return snapshot.iterator(partFilter, rowFilter, columns);
  }
}
