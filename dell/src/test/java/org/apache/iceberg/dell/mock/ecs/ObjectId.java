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
package org.apache.iceberg.dell.mock.ecs;

import java.util.Comparator;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;

public class ObjectId implements Comparable<ObjectId> {
  public static final Comparator<ObjectId> COMPARATOR =
      Comparator.<ObjectId, String>comparing(id -> id.bucket).thenComparing(id -> id.name);

  public final String bucket;
  public final String name;

  public ObjectId(String bucket, String name) {
    this.bucket = bucket;
    this.name = name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ObjectId objectId = (ObjectId) o;
    return Objects.equal(bucket, objectId.bucket) && Objects.equal(name, objectId.name);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(bucket, name);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("bucket", bucket).add("name", name).toString();
  }

  @Override
  public int compareTo(ObjectId o) {
    return COMPARATOR.compare(this, o);
  }
}
