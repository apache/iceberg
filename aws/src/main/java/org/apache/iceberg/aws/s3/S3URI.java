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
package org.apache.iceberg.aws.s3;

import java.util.Map;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * This class represents a fully qualified location in S3 for input/output operations expressed as
 * as URI. This implementation is provided to ensure compatibility with Hadoop Path implementations
 * that may introduce encoding issues with native URI implementation. If the bucket in the location
 * has an access point in the mapping, the access point is used to perform all the S3 operations.
 *
 * <p>Note: Path-style access is deprecated and not supported by this implementation.
 */
class S3URI {
  private static final String SCHEME_DELIM = "://";
  private static final String PATH_DELIM = "/";

  private final String location;
  private final String scheme;
  private final String bucket;
  private final String key;

  /**
   * Creates a new S3URI in the form of scheme://bucket/key?query#fragment
   *
   * <p>The URI supports any valid URI schemes to be backwards compatible with s3a and s3n, and also
   * allows users to use S3FileIO with other S3-compatible object storage services like GCS.
   *
   * @param location fully qualified URI
   */
  S3URI(String location) {
    this(location, ImmutableMap.of());
  }

  /**
   * Creates a new S3URI in the form of scheme://(bucket|accessPoint)/key?query#fragment with
   * additional information on accessPoints.
   *
   * <p>The URI supports any valid URI schemes to be backwards compatible with s3a and s3n, and also
   * allows users to use S3FileIO with other S3-compatible object storage services like GCS.
   *
   * @param location fully qualified URI
   * @param bucketToAccessPointMapping contains mapping of bucket to access point
   */
  S3URI(String location, Map<String, String> bucketToAccessPointMapping) {
    Preconditions.checkNotNull(location, "Location cannot be null.");

    this.location = location;
    String[] schemeSplit = location.split(SCHEME_DELIM, -1);
    ValidationException.check(
        schemeSplit.length == 2, "Invalid S3 URI, cannot determine scheme: %s", location);
    this.scheme = schemeSplit[0];

    String[] authoritySplit = schemeSplit[1].split(PATH_DELIM, 2);

    this.bucket =
        bucketToAccessPointMapping == null
            ? authoritySplit[0]
            : bucketToAccessPointMapping.getOrDefault(authoritySplit[0], authoritySplit[0]);

    // Note: AWS UI (as an example) embeds special chars like `?` and `#` in `s3://` URIs without
    // encoding. This technically does not match the URI spec per RFC 3986, but it is worth
    // supporting to ensure interoperability for S3FileIO. Therefore, the whole sub-string after the
    // "authority" part is treated as the "key" without interpreting "query" and "fragment" parts
    // defined by the RFC.
    this.key = authoritySplit.length > 1 ? authoritySplit[1] : "";
  }

  /** Returns S3 bucket name. */
  public String bucket() {
    return bucket;
  }

  /** Returns S3 object key name. */
  public String key() {
    return key;
  }

  /** Returns original, unmodified S3 URI location. */
  public String location() {
    return location;
  }

  /**
   * Returns the original scheme provided in the location.
   *
   * @return uri scheme
   */
  public String scheme() {
    return scheme;
  }

  @Override
  public String toString() {
    return location;
  }
}
