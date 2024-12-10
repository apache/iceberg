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
package org.apache.iceberg.aws.s3tables;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.hash.HashCode;
import org.apache.iceberg.relocated.com.google.common.hash.HashFunction;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
import org.apache.iceberg.util.LocationUtil;

/**
 * This location provider provides data locations that are optimized for S3 performance. Both
 * General Purpose buckets and Directory buckets will see better throughput and autoscaling behavior
 * than using the generic ObjectStoreLocationProvider.
 *
 * <p>The data location is resolved as follows. Data files are written directly at this path with no
 * other intermediate directories created.
 *
 * <ol>
 *   <li>{@link TableProperties#WRITE_DATA_LOCATION}
 *   <li><code>tableLocation + "/data"</code>
 * </ol>
 *
 * The data file is placed immediately under the data location. Partition names are <b>not</b>
 * included. The data filename is prefixed with a 24-character binary hash, which ensures that files
 * written to S3 are equally distributed across many prefixes in the S3 bucket.
 *
 * <p>For example, with tableLocation <code>s3://my-bucket/my-table</code>, an example data file
 * could look like <code>
 * s3://my-bucket/my-table/data/011101101010001111101000-00000-0-5affc076-96a4-48f2-9cd2-d5efbc9f0c94-00001.parquet
 * </code>.
 */
public class S3TablesLocationProvider implements LocationProvider {
  private static final HashFunction HASH_FUNC = Hashing.murmur3_32_fixed();
  // the starting index of the lower 24-bits of a 32-bit binary string
  private static final int HASH_BINARY_STRING_START_INDEX = 8;
  private final String storageLocation;

  public S3TablesLocationProvider(String tableLocation, Map<String, String> properties) {
    this.storageLocation = LocationUtil.stripTrailingSlash(dataLocation(properties, tableLocation));
  }

  @Override
  public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
    return newDataLocation(filename);
  }

  @Override
  public String newDataLocation(String filename) {
    String hash = computeHash(filename);
    return String.format("%s/%s-%s", storageLocation, hash, filename);
  }

  private static String dataLocation(Map<String, String> properties, String tableLocation) {
    String dataLocation = properties.get(TableProperties.WRITE_DATA_LOCATION);
    if (dataLocation == null) {
      dataLocation = String.format("%s/data", tableLocation);
    }
    return dataLocation;
  }

  @VisibleForTesting
  String computeHash(String fileName) {
    HashCode hashCode = HASH_FUNC.hashString(fileName, StandardCharsets.UTF_8);
    int hash = hashCode.asInt();

    // {@link Integer#toBinaryString} excludes leading zeros, which we want to preserve.
    // force the first bit to be set to get around that.
    String hashAsBinaryString = Integer.toBinaryString(hash | Integer.MIN_VALUE);
    return hashAsBinaryString.substring(HASH_BINARY_STRING_START_INDEX);
  }
}
