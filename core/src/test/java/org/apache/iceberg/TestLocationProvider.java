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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestLocationProvider extends TestBase {

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
            "Provided implementation for dynamic instantiation should implement %s.",
            LocationProvider.class);
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
  public void testObjectStorageLocationProviderThrowOnDeprecatedProperties() {
    String objectPath = "s3://random/object/location";
    table
        .updateProperties()
        .set(TableProperties.OBJECT_STORE_ENABLED, "true")
        .set(TableProperties.WRITE_FOLDER_STORAGE_LOCATION, objectPath)
        .commit();

    assertThatThrownBy(() -> table.locationProvider().newDataLocation("file"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Property 'write.folder-storage.path' has been deprecated and will be removed in 2.0, use 'write.data.path' instead.");

    table
        .updateProperties()
        .set(TableProperties.OBJECT_STORE_PATH, objectPath)
        .remove(TableProperties.WRITE_FOLDER_STORAGE_LOCATION)
        .commit();

    assertThatThrownBy(() -> table.locationProvider().newDataLocation("file"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Property 'write.object-storage.path' has been deprecated and will be removed in 2.0, use 'write.data.path' instead.");
  }

  @TestTemplate
  public void testDefaultStorageLocationProviderThrowOnDeprecatedProperties() {
    String folderPath = "s3://random/folder/location";
    table
        .updateProperties()
        .set(TableProperties.OBJECT_STORE_ENABLED, "false")
        .set(TableProperties.WRITE_FOLDER_STORAGE_LOCATION, folderPath)
        .commit();

    assertThatThrownBy(() -> table.locationProvider().newDataLocation("file"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Property 'write.folder-storage.path' has been deprecated and will be removed in 2.0, use 'write.data.path' instead.");
  }

  @TestTemplate
  public void testObjectStorageWithinTableLocation() {
    table.updateProperties().set(TableProperties.OBJECT_STORE_ENABLED, "true").commit();

    String fileLocation = table.locationProvider().newDataLocation("test.parquet");
    String relativeLocation = fileLocation.replaceFirst(table.location(), "");
    List<String> parts = Splitter.on("/").splitToList(relativeLocation);
    assertThat(parts).hasSize(7);
    assertThat(parts).first().asString().isEmpty();
    assertThat(parts).element(1).asString().isEqualTo("data");
    // entropy dirs in the middle
    assertThat(parts).elements(2, 3, 4, 5).asString().isNotEmpty();
    assertThat(parts).element(6).asString().isEqualTo("test.parquet");
  }

  @TestTemplate
  public void testEncodedFieldNameInPartitionPath() {
    // Update the table to use a string field for partitioning with special characters in the name
    table.updateProperties().set(TableProperties.OBJECT_STORE_ENABLED, "true").commit();
    table.updateSchema().addColumn("data#1", Types.StringType.get()).commit();
    table.updateSpec().addField("data#1").commit();

    // Use a partition value that has a special character
    StructLike partitionData = TestHelpers.CustomRow.of(0, "val#1");

    String fileLocation =
        table.locationProvider().newDataLocation(table.spec(), partitionData, "test.parquet");
    List<String> parts = Splitter.on("/").splitToList(fileLocation);
    String partitionString = parts.get(parts.size() - 2);

    assertThat(partitionString).isEqualTo("data%231=val%231");
  }

  @TestTemplate
  public void testExcludePartitionInPath() {
    // Update the table to use a string field for partitioning with special characters in the name
    table.updateProperties().set(TableProperties.OBJECT_STORE_ENABLED, "true").commit();
    table
        .updateProperties()
        .set(TableProperties.WRITE_OBJECT_STORE_PARTITIONED_PATHS, "false")
        .commit();

    // Use a partition value that has a special character
    StructLike partitionData = TestHelpers.CustomRow.of(0, "val");
    String fileLocation =
        table.locationProvider().newDataLocation(table.spec(), partitionData, "test.parquet");

    // no partition values included in the path and last part of entropy is seperated with "-"
    assertThat(fileLocation).endsWith("/data/0110/1010/0011/11101000-test.parquet");
  }

  @TestTemplate
  public void testHashInjection() {
    table.updateProperties().set(TableProperties.OBJECT_STORE_ENABLED, "true").commit();
    assertThat(table.locationProvider().newDataLocation("a"))
        .endsWith("/data/0101/0110/1001/10110010/a");
    assertThat(table.locationProvider().newDataLocation("b"))
        .endsWith("/data/1110/0111/1110/00000011/b");
    assertThat(table.locationProvider().newDataLocation("c"))
        .endsWith("/data/0010/1101/0110/01011111/c");
    assertThat(table.locationProvider().newDataLocation("d"))
        .endsWith("/data/1001/0001/0100/01110011/d");
  }
}
