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

import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * This class represents a fully qualified location in S3 for input/output
 * operations expressed as as URI.  This implementation is provided to
 * ensure compatibility with Hadoop Path implementations that may introduce
 * encoding issues with native URI implementation.
 *
 * Note: Path-style access is deprecated and not supported by this
 * implementation.
 */
class S3URI {
  private static final String SCHEME_DELIM = "://";
  private static final String PATH_DELIM = "/";
  private static final String QUERY_DELIM = "\\?";
  private static final String FRAGMENT_DELIM = "#";
  private static final Set<String> VALID_SCHEMES = ImmutableSet.of("https", "s3", "s3a", "s3n");

  private final String location;
  private final String bucket;
  private final String key;

  /**
   * Creates a new S3URI based on the bucket and key parsed from the location as defined in:
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro
   *
   * Supported access styles are Virtual Hosted addresses and s3://... URIs with additional
   * 's3n' and 's3a' schemes supported for backwards compatibility.
   *
   * @param location fully qualified URI
   */
  S3URI(String location) {
    Preconditions.checkNotNull(location, "Location cannot be null.");

    this.location = location;
    String [] schemeSplit = location.split(SCHEME_DELIM, -1);
    ValidationException.check(schemeSplit.length == 2, "Invalid S3 URI: %s", location);

    String scheme = schemeSplit[0];
    ValidationException.check(VALID_SCHEMES.contains(scheme.toLowerCase()), "Invalid scheme: %s", scheme);

    String [] authoritySplit = schemeSplit[1].split(PATH_DELIM, 2);
    ValidationException.check(authoritySplit.length == 2, "Invalid S3 URI: %s", location);
    ValidationException.check(!authoritySplit[1].trim().isEmpty(), "Invalid S3 key: %s", location);
    this.bucket = authoritySplit[0];

    // Strip query and fragment if they exist
    String path = authoritySplit[1];
    path = path.split(QUERY_DELIM, -1)[0];
    path = path.split(FRAGMENT_DELIM, -1)[0];
    this.key = path;
  }

  /**
   * Returns S3 bucket name.
   */
  public String bucket() {
    return bucket;
  }

  /**
   * Returns S3 object key name.
   */
  public String key() {
    return key;
  }

  /**
   * Returns original, unmodified S3 URI location.
   */
  public String location() {
    return location;
  }

  @Override
  public String toString() {
    return location;
  }
}
