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
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.hash.HashCode;
import org.apache.iceberg.relocated.com.google.common.hash.HashFunction;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
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

  static class DefaultLocationProvider implements LocationProvider {
    private final String dataLocation;

    DefaultLocationProvider(String tableLocation, Map<String, String> properties) {
      this.dataLocation = LocationUtil.stripTrailingSlash(dataLocation(properties, tableLocation));
    }

    private static String dataLocation(Map<String, String> properties, String tableLocation) {
      String dataLocation = properties.get(TableProperties.WRITE_DATA_LOCATION);
      if (dataLocation == null) {
        dataLocation = properties.get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
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
    private static final BaseEncoding BASE64_ENCODER = BaseEncoding.base64Url().omitPadding();
    private static final ThreadLocal<byte[]> TEMP = ThreadLocal.withInitial(() -> new byte[4]);
    private final String storageLocation;
    private final String context;

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
    }

    private static String dataLocation(Map<String, String> properties, String tableLocation) {
      String dataLocation = properties.get(TableProperties.WRITE_DATA_LOCATION);
      if (dataLocation == null) {
        dataLocation = properties.get(TableProperties.OBJECT_STORE_PATH);
        if (dataLocation == null) {
          dataLocation = properties.get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
          if (dataLocation == null) {
            dataLocation = String.format("%s/data", tableLocation);
          }
        }
      }
      return dataLocation;
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      return newDataLocation(String.format("%s/%s", spec.partitionToPath(partitionData), filename));
    }

    @Override
    public String newDataLocation(String filename) {
      String hash = computeHash(filename);
      if (context != null) {
        return String.format("%s/%s/%s/%s", storageLocation, hash, context, filename);
      } else {
        return String.format("%s/%s/%s", storageLocation, hash, filename);
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
      byte[] bytes = TEMP.get();
      HashCode hash = HASH_FUNC.hashString(fileName, StandardCharsets.UTF_8);
      hash.writeBytesTo(bytes, 0, 4);
      return BASE64_ENCODER.encode(bytes);
    }
  }
}
