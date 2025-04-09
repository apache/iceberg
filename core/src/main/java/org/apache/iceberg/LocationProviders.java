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
package org.apache.iceberg;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.hash.HashCode;
import org.apache.iceberg.relocated.com.google.common.hash.HashFunction;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.PropertyUtil;

public class LocationProviders {

  private LocationProviders() {}

  public static LocationProvider locationsFor(
      String inputLocation, Map<String, String> properties) {
    String location = LocationUtil.stripTrailingSlash(inputLocation);
    if (properties.containsKey(TableProperties.WRITE_LOCATION_PROVIDER_IMPL)) {
      String impl = properties.get(TableProperties.WRITE_LOCATION_PROVIDER_IMPL);
      DynConstructors.Ctor<LocationProvider> ctor;
      try {
        ctor =
            DynConstructors.builder(LocationProvider.class)
                .impl(impl, String.class, Map.class)
                .impl(impl)
                .buildChecked(); // fall back to no-arg constructor
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(
            String.format(
                "Unable to find a constructor for implementation %s of %s. "
                    + "Make sure the implementation is in classpath, and that it either "
                    + "has a public no-arg constructor or a two-arg constructor "
                    + "taking in the string base table location and its property string map.",
                impl, LocationProvider.class),
            e);
      }
      try {
        return ctor.newInstance(location, properties);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            String.format(
                "Provided implementation for dynamic instantiation should implement %s.",
                LocationProvider.class),
            e);
      }
    } else if (PropertyUtil.propertyAsBoolean(
        properties,
        TableProperties.OBJECT_STORE_ENABLED,
        TableProperties.OBJECT_STORE_ENABLED_DEFAULT)) {
      return new ObjectStoreLocationProvider(location, properties);
    } else {
      return new DefaultLocationProvider(location, properties);
    }
  }

  private static final Set<String> DEPRECATED_PROPERTIES =
      ImmutableSet.of(
          TableProperties.OBJECT_STORE_PATH, TableProperties.WRITE_FOLDER_STORAGE_LOCATION);

  private static String getAndCheckLegacyLocation(Map<String, String> properties, String key) {
    String value = properties.get(key);

    if (value != null && DEPRECATED_PROPERTIES.contains(key)) {
      throw new IllegalArgumentException(
          String.format(
              "Property '%s' has been deprecated and will be removed in 2.0, use '%s' instead.",
              key, TableProperties.WRITE_DATA_LOCATION));
    }

    return value;
  }

  static class DefaultLocationProvider implements LocationProvider {
    private final String dataLocation;

    DefaultLocationProvider(String tableLocation, Map<String, String> properties) {
      this.dataLocation = LocationUtil.stripTrailingSlash(dataLocation(properties, tableLocation));
    }

    private static String dataLocation(Map<String, String> properties, String tableLocation) {
      String dataLocation =
          getAndCheckLegacyLocation(properties, TableProperties.WRITE_DATA_LOCATION);
      if (dataLocation == null) {
        dataLocation =
            getAndCheckLegacyLocation(properties, TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
        if (dataLocation == null) {
          dataLocation = String.format("%s/data", tableLocation);
        }
      }
      return dataLocation;
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      return String.format("%s/%s/%s", dataLocation, spec.partitionToPath(partitionData), filename);
    }

    @Override
    public String newDataLocation(String filename) {
      return String.format("%s/%s", dataLocation, filename);
    }
  }

  static class ObjectStoreLocationProvider implements LocationProvider {

    private static final HashFunction HASH_FUNC = Hashing.murmur3_32_fixed();
    // Length of entropy generated in the file location
    private static final int HASH_BINARY_STRING_BITS = 20;
    // Entropy generated will be divided into dirs with this lengths
    private static final int ENTROPY_DIR_LENGTH = 4;
    // Will create DEPTH many dirs from the entropy
    private static final int ENTROPY_DIR_DEPTH = 3;
    private final String storageLocation;
    private final String context;
    private final boolean includePartitionPaths;

    ObjectStoreLocationProvider(String tableLocation, Map<String, String> properties) {
      this.storageLocation =
          LocationUtil.stripTrailingSlash(dataLocation(properties, tableLocation));
      // if the storage location is within the table prefix, don't add table and database name
      // context
      if (storageLocation.startsWith(tableLocation)) {
        this.context = null;
      } else {
        this.context = pathContext(tableLocation);
      }
      this.includePartitionPaths =
          PropertyUtil.propertyAsBoolean(
              properties,
              TableProperties.WRITE_OBJECT_STORE_PARTITIONED_PATHS,
              TableProperties.WRITE_OBJECT_STORE_PARTITIONED_PATHS_DEFAULT);
    }

    private static String dataLocation(Map<String, String> properties, String tableLocation) {
      String dataLocation =
          getAndCheckLegacyLocation(properties, TableProperties.WRITE_DATA_LOCATION);
      if (dataLocation == null) {
        dataLocation = getAndCheckLegacyLocation(properties, TableProperties.OBJECT_STORE_PATH);
        if (dataLocation == null) {
          dataLocation =
              getAndCheckLegacyLocation(properties, TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
          if (dataLocation == null) {
            dataLocation = String.format("%s/data", tableLocation);
          }
        }
      }
      return dataLocation;
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      if (includePartitionPaths) {
        return newDataLocation(
            String.format("%s/%s", spec.partitionToPath(partitionData), filename));
      } else {
        return newDataLocation(filename);
      }
    }

    @Override
    public String newDataLocation(String filename) {
      String hash = computeHash(filename);
      if (context != null) {
        return String.format("%s/%s/%s/%s", storageLocation, hash, context, filename);
      } else {
        // if partition paths are included, add last part of entropy as dir before partition names
        if (includePartitionPaths) {
          return String.format("%s/%s/%s", storageLocation, hash, filename);
        } else {
          // if partition paths are not included, append last part of entropy with `-` to file name
          return String.format("%s/%s-%s", storageLocation, hash, filename);
        }
      }
    }

    private static String pathContext(String tableLocation) {
      Path dataPath = new Path(tableLocation);
      Path parent = dataPath.getParent();
      String resolvedContext;
      if (parent != null) {
        // remove the data folder
        resolvedContext = String.format("%s/%s", parent.getName(), dataPath.getName());
      } else {
        resolvedContext = dataPath.getName();
      }

      Preconditions.checkState(
          !resolvedContext.endsWith("/"), "Path context must not end with a slash.");

      return resolvedContext;
    }

    private String computeHash(String fileName) {
      HashCode hashCode = HASH_FUNC.hashString(fileName, StandardCharsets.UTF_8);

      // {@link Integer#toBinaryString} excludes leading zeros, which we want to preserve.
      // force the first bit to be set to get around that.
      String hashAsBinaryString = Integer.toBinaryString(hashCode.asInt() | Integer.MIN_VALUE);
      // Limit hash length to HASH_BINARY_STRING_BITS
      String hash =
          hashAsBinaryString.substring(hashAsBinaryString.length() - HASH_BINARY_STRING_BITS);
      return dirsFromHash(hash);
    }

    /**
     * Divides hash into directories for optimized orphan removal operation using ENTROPY_DIR_DEPTH
     * and ENTROPY_DIR_LENGTH
     *
     * @param hash 10011001100110011001
     * @return 1001/1001/1001/10011001 with depth 3 and length 4
     */
    private String dirsFromHash(String hash) {
      StringBuilder hashWithDirs = new StringBuilder();

      for (int i = 0; i < ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH; i += ENTROPY_DIR_LENGTH) {
        if (i > 0) {
          hashWithDirs.append("/");
        }
        hashWithDirs.append(hash, i, Math.min(i + ENTROPY_DIR_LENGTH, hash.length()));
      }

      if (hash.length() > ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH) {
        hashWithDirs
            .append("/")
            .append(hash, ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH, hash.length());
      }

      return hashWithDirs.toString();
    }
  }
}
