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

package org.apache.iceberg.dell;

import java.net.URI;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * An immutable record class of ECS location
 */
public class EcsURI {

  private static final Set<String> VALID_SCHEME = ImmutableSet.of("ecs", "s3", "s3a", "s3n");

  public static EcsURI create(String location) {
    URI uri = URI.create(location);
    if (!VALID_SCHEME.contains(uri.getScheme().toLowerCase())) {
      throw new ValidationException("Invalid ecs location: %s", location);
    }
    String bucket = uri.getHost();
    String name = uri.getPath().replaceAll("^/*", "");
    return new EcsURI(bucket, name);
  }

  private final String bucket;
  private final String name;

  public EcsURI(String bucket, String name) {
    Preconditions.checkNotNull(bucket == null, "Bucket %s can not be null", bucket);
    Preconditions.checkNotNull(name == null, "Object name %s can not be null", name);

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
    return String.format("ecs://%s/%s", bucket, name);
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
