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
package org.apache.iceberg.spark.source;

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;

public class NestedRecord {
  private long innerId;
  private String innerName;

  public NestedRecord() {}

  public NestedRecord(long innerId, String innerName) {
    this.innerId = innerId;
    this.innerName = innerName;
  }

  public long getInnerId() {
    return innerId;
  }

  public String getInnerName() {
    return innerName;
  }

  public void setInnerId(long iId) {
    innerId = iId;
  }

  public void setInnerName(String name) {
    innerName = name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NestedRecord that = (NestedRecord) o;
    return innerId == that.innerId && Objects.equal(innerName, that.innerName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(innerId, innerName);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("innerId", innerId)
        .add("innerName", innerName)
        .toString();
  }
}
