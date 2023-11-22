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
package org.apache.iceberg.tencentcloud.cos;

import com.qcloud.cos.internal.BucketNameUtils;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

public class CosURI {
  private static final String SCHEME_DELIM = "://";
  private static final String PATH_DELIM = "/";
  private static final String QUERY_DELIM = "\\?";
  private static final String FRAGMENT_DELIM = "#";
  private static final Set<String> VALID_SCHEMES = ImmutableSet.of("cos", "cosn");
  private final String location;
  private final String bucket;
  private final String key;

  public CosURI(String location) {
    Preconditions.checkArgument(location != null, "Invalid Cos location: null");

    this.location = location;
    String[] schemeSplit = location.split(SCHEME_DELIM, -1);
    ValidationException.check(schemeSplit.length == 2, "Invalid Cos location: %s", location);

    String scheme = schemeSplit[0];
    ValidationException.check(
        VALID_SCHEMES.contains(scheme.toLowerCase()),
        "Invalid scheme: %s in Cos location %s",
        scheme,
        location);

    String[] authoritySplit = schemeSplit[1].split(PATH_DELIM, 2);
    ValidationException.check(
        authoritySplit.length == 2, "Invalid bucket or key in Cos location: %s", location);
    ValidationException.check(
        !authoritySplit[1].trim().isEmpty(), "Missing key in Cos location: %s", location);
    this.bucket = authoritySplit[0];
    BucketNameUtils.validateBucketName(bucket);

    // Strip query and fragment if they exist
    String path = PATH_DELIM + authoritySplit[1];
    path = path.split(QUERY_DELIM, -1)[0];
    path = path.split(FRAGMENT_DELIM, -1)[0];
    this.key = pathToKey(new Path(path));
  }

  public String bucket() {
    return bucket;
  }

  public String key() {
    return key;
  }

  public String location() {
    return location;
  }

  @Override
  public String toString() {
    return location;
  }

  private static String pathToKey(Path path) {
    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      // allow uris without trailing slash after bucket to refer to root,
      // like cosn://mybucket
      return "";
    }
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }
    return path.toUri().getPath();
  }
}
