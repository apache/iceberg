/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.dell;

import java.util.Objects;

/**
 * An immutable record class of ECS location
 */
public class EcsURI {

  private final String bucket;
  private final String name;

  public EcsURI(String bucket, String name) {
    if (bucket == null || name == null) {
      throw new IllegalArgumentException(String.format("bucket %s and key %s must be not null", bucket, name));
    }
    this.bucket = bucket;
    this.name = name;
  }

  public String getBucket() {
    return bucket;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return LocationUtils.toString(bucket, name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EcsURI uri = (EcsURI) o;
    return Objects.equals(bucket, uri.bucket) && Objects.equals(name, uri.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucket, name);
  }
}
