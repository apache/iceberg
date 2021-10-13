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

package org.apache.iceberg.aliyun.oss;

import com.aliyun.oss.internal.OSSUtils;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * This class represents a fully qualified location in OSS for input/output
 * operations expressed as as URI.  This implementation is provided to
 * ensure compatibility with Hadoop Path implementations that may introduce
 * encoding issues with native URI implementation.
 *
 * Note: Path-style access is deprecated and not supported by this
 * implementation.
 */
public class AliyunOSSURI {
  private static final String SCHEMA_DELIM = "://";
  private static final String PATH_DELIM = "/";
  private static final String QUERY_DELIM = "\\?";
  private static final String FRAGMENT_DELIM = "#";
  private static final Set<String> VALID_SCHEMAS = ImmutableSet.of("https", "oss");
  private final String location;
  private String bucket;
  private String key;

  /**
   * Creates a new Aliyun OSSURI based on the bucket and key parsed from the location as below:
   * [scheme:]scheme-specific-part[#fragment]
   * [scheme:][//bucket][object key][?query][#fragment]
   * <p>
   * Supported access styles are https and oss://... URIs.
   *
   * @param location fully qualified URI.
   */
  public AliyunOSSURI(String location) {
    Preconditions.checkNotNull(location, "OSS location cannot be null.");

    this.location = location;
    String[] schemaSplit = location.split(SCHEMA_DELIM, -1);
    ValidationException.check(schemaSplit.length == 2, "Invalid OSS location: %s", location);

    String schema = schemaSplit[0];
    ValidationException.check(VALID_SCHEMAS.contains(schema.toLowerCase()), "Invalid schema: %s", schema);

    String[] authoritySplit = schemaSplit[1].split(PATH_DELIM, 2);
    ValidationException.check(authoritySplit.length == 2,
            "Invalid bucket or key in OSS location: %s", location);
    ValidationException.check(!authoritySplit[1].trim().isEmpty(),
            "Invalid key in OSS location: %s", location);
    this.bucket = authoritySplit[0];
    OSSUtils.ensureBucketNameValid(bucket);

    // Strip query and fragment if they exist
    String path = authoritySplit[1];
    path = path.split(QUERY_DELIM, -1)[0];
    path = path.split(FRAGMENT_DELIM, -1)[0];
    this.key = path;
    OSSUtils.ensureObjectKeyValid(key);
  }

  /**
   * Return OSS bucket name.
   */
  public String getBucket() {
    return bucket;
  }

  /**
   * Return OSS object key name.
   */
  public String getKey() {
    return key;
  }

  @Override
  public String toString() {
    return location;
  }
}
