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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestLocationProvider extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  // publicly visible for testing to be dynamically loaded
  public static class TwoArgDynamicallyLoadedLocationProvider implements LocationProvider {
    String tableLocation;
    Map<String, String> properties;

    public TwoArgDynamicallyLoadedLocationProvider(
        String tableLocation, Map<String, String> properties) {
      this.tableLocation = tableLocation;
      this.properties = properties;
    }

    @Override
    public String newDataLocation(String filename) {
      return String.format("%s/test_custom_provider/%s", this.tableLocation, filename);
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      throw new RuntimeException("Test custom provider does not expect any invocation");
    }
  }

  // publicly visible for testing to be dynamically loaded
  public static class NoArgDynamicallyLoadedLocationProvider implements LocationProvider {
    // No-arg public constructor

    @Override
    public String newDataLocation(String filename) {
      return String.format("test_no_arg_provider/%s", filename);
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      throw new RuntimeException("Test custom provider does not expect any invocation");
    }
  }

  // publicly visible for testing to be dynamically loaded
  public static class InvalidArgTypesDynamicallyLoadedLocationProvider implements LocationProvider {

    public InvalidArgTypesDynamicallyLoadedLocationProvider(Integer bogusArg1, String bogusArg2) {}

    @Override
    public String newDataLocation(String filename) {
      throw new RuntimeException("Invalid provider should have not been instantiated!");
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      throw new RuntimeException("Invalid provider should have not been instantiated!");
    }
  }

  // publicly visible for testing to be dynamically loaded
  public static class InvalidNoInterfaceDynamicallyLoadedLocationProvider {
    // Default no-arg constructor is present, but does not impelemnt interface LocationProvider
  }

  @TestTemplate
  public void testDefaultLocationProvider() {
    this.table.updateProperties().commit();

    this.table.locationProvider().newDataLocation("my_file");
    assertThat(this.table.locationProvider().newDataLocation("my_file"))
        .isEqualTo(String.format("%s/data/%s", this.table.location(), "my_file"));
  }

  @TestTemplate
  public void testDefaultLocationProviderWithCustomDataLocation() {
    this.table.updateProperties().set(TableProperties.WRITE_DATA_LOCATION, "new_location").commit();

    this.table.locationProvider().newDataLocation("my_file");
    assertThat(this.table.locationProvider().newDataLocation("my_file"))
        .isEqualTo("new_location/my_file");
  }

  @TestTemplate
  public void testNoArgDynamicallyLoadedLocationProvider() {
    String invalidImpl =
        String.format(
            "%s$%s",
            this.getClass().getCanonicalName(),
            NoArgDynamicallyLoadedLocationProvider.class.getSimpleName());
    this.table
        .updateProperties()
        .set(TableProperties.WRITE_LOCATION_PROVIDER_IMPL, invalidImpl)
        .commit();

    assertThat(this.table.locationProvider().newDataLocation("my_file"))
        .isEqualTo("test_no_arg_provider/my_file");
  }

  @TestTemplate
  public void testTwoArgDynamicallyLoadedLocationProvider() {
    this.table
        .updateProperties()
        .set(
            TableProperties.WRITE_LOCATION_PROVIDER_IMPL,
            String.format(
                "%s$%s",
                this.getClass().getCanonicalName(),
                TwoArgDynamicallyLoadedLocationProvider.class.getSimpleName()))
        .commit();

    assertThat(this.table.locationProvider())
        .as("Table should load impl defined in its properties")
        .isInstanceOf(TwoArgDynamicallyLoadedLocationProvider.class);

    assertThat(this.table.locationProvider().newDataLocation("my_file"))
        .isEqualTo(String.format("%s/test_custom_provider/%s", this.table.location(), "my_file"));
  }

  @TestTemplate
  public void testDynamicallyLoadedLocationProviderNotFound() {
    String nonExistentImpl =
        String.format(
            "%s$NonExistent%s",
            this.getClass().getCanonicalName(),
            TwoArgDynamicallyLoadedLocationProvider.class.getSimpleName());
    this.table
        .updateProperties()
        .set(TableProperties.WRITE_LOCATION_PROVIDER_IMPL, nonExistentImpl)
        .commit();

    assertThatThrownBy(() -> table.locationProvider())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            String.format(
                "Unable to find a constructor for implementation %s of %s. ",
                nonExistentImpl, LocationProvider.class))
        .hasMessageEndingWith(
            "Make sure the implementation is in classpath, and that it either "
                + "has a public no-arg constructor or a two-arg constructor "
                + "taking in the string base table location and its property string map.");
  }

  @TestTemplate
  public void testInvalidNoInterfaceDynamicallyLoadedLocationProvider() {
    String invalidImpl =
        String.format(
            "%s$%s",
            this.getClass().getCanonicalName(),
            InvalidNoInterfaceDynamicallyLoadedLocationProvider.class.getSimpleName());
    this.table
        .updateProperties()
        .set(TableProperties.WRITE_LOCATION_PROVIDER_IMPL, invalidImpl)
        .commit();

    assertThatThrownBy(() -> table.locationProvider())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Provided implementation for dynamic instantiation should implement %s.",
                LocationProvider.class));
  }

  @TestTemplate
  public void testInvalidArgTypesDynamicallyLoadedLocationProvider() {
    String invalidImpl =
        String.format(
            "%s$%s",
            this.getClass().getCanonicalName(),
            InvalidArgTypesDynamicallyLoadedLocationProvider.class.getSimpleName());
    this.table
        .updateProperties()
        .set(TableProperties.WRITE_LOCATION_PROVIDER_IMPL, invalidImpl)
        .commit();

    assertThatThrownBy(() -> table.locationProvider())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            String.format(
                "Unable to find a constructor for implementation %s of %s. ",
                invalidImpl, LocationProvider.class));
  }

  @TestTemplate
  public void testObjectStorageLocationProviderPathResolution() {
    table.updateProperties().set(TableProperties.OBJECT_STORE_ENABLED, "true").commit();

    assertThat(table.locationProvider().newDataLocation("file"))
        .as("default data location should be used when object storage path not set")
        .contains(table.location() + "/data");

    String folderPath = "s3://random/folder/location";
    table
        .updateProperties()
        .set(TableProperties.WRITE_FOLDER_STORAGE_LOCATION, folderPath)
        .commit();

    assertThat(table.locationProvider().newDataLocation("file"))
        .as("folder storage path should be used when set")
        .contains(folderPath);

    String objectPath = "s3://random/object/location";
    table.updateProperties().set(TableProperties.OBJECT_STORE_PATH, objectPath).commit();

    assertThat(table.locationProvider().newDataLocation("file"))
        .as("object storage path should be used when set")
        .contains(objectPath);

    String dataPath = "s3://random/data/location";
    table.updateProperties().set(TableProperties.WRITE_DATA_LOCATION, dataPath).commit();

    assertThat(table.locationProvider().newDataLocation("file"))
        .as("write data path should be used when set")
        .contains(dataPath);
  }

  @TestTemplate
  public void testDefaultStorageLocationProviderPathResolution() {
    table.updateProperties().set(TableProperties.OBJECT_STORE_ENABLED, "false").commit();

    assertThat(table.locationProvider().newDataLocation("file"))
        .as("default data location should be used when object storage path not set")
        .contains(table.location() + "/data");

    String folderPath = "s3://random/folder/location";
    table
        .updateProperties()
        .set(TableProperties.WRITE_FOLDER_STORAGE_LOCATION, folderPath)
        .commit();

    assertThat(table.locationProvider().newDataLocation("file"))
        .as("folder storage path should be used when set")
        .contains(folderPath);

    String dataPath = "s3://random/data/location";
    table.updateProperties().set(TableProperties.WRITE_DATA_LOCATION, dataPath).commit();

    assertThat(table.locationProvider().newDataLocation("file"))
        .as("write data path should be used when set")
        .contains(dataPath);
  }

  @TestTemplate
  public void testObjectStorageWithinTableLocation() {
    table.updateProperties().set(TableProperties.OBJECT_STORE_ENABLED, "true").commit();

    String fileLocation = table.locationProvider().newDataLocation("test.parquet");
    String relativeLocation = fileLocation.replaceFirst(table.location(), "");
    List<String> parts = Splitter.on("/").splitToList(relativeLocation);

    assertThat(parts).hasSize(4);
    assertThat(parts).first().asString().isEmpty();
    assertThat(parts).element(1).asString().isEqualTo("data");
    assertThat(parts).element(2).asString().isNotEmpty();
    assertThat(parts).element(3).asString().isEqualTo("test.parquet");
  }
}
