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

import java.util.Map;
import org.apache.iceberg.io.LocationProvider;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestLocationProvider extends TableTestBase {
  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { 1 },
        new Object[] { 2 },
    };
  }

  public TestLocationProvider(int formatVersion) {
    super(formatVersion);
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  // publicly visible for testing to be dynamically loaded
  public static class DynamicallyLoadedLocationProvider implements LocationProvider {
    String tableLocation;
    Map<String, String> properties;

    public DynamicallyLoadedLocationProvider(String tableLocation, Map<String, String> properties) {
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
  public static class InvalidDynamicallyLoadedLocationProvider implements LocationProvider {
    // No public constructor

    @Override
    public String newDataLocation(String filename) {
      throw new IllegalStateException("Should have never been instantiated");
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      throw new IllegalStateException("Should have never been instantiated");
    }
  }

  @Test
  public void testDefaultLocationProvider() {
    this.table.updateProperties()
        .commit();

    this.table.locationProvider().newDataLocation("my_file");
    Assert.assertEquals(
        "Default data path should have table location as root",
        String.format("%s/data/%s", this.table.location(), "my_file"),
        this.table.locationProvider().newDataLocation("my_file")
    );
  }

  @Test
  public void testDefaultLocationProviderWithCustomDataLocation() {
    this.table.updateProperties()
        .set(TableProperties.WRITE_NEW_DATA_LOCATION, "new_location")
        .commit();

    this.table.locationProvider().newDataLocation("my_file");
    Assert.assertEquals(
        "Default location provider should allow custom path location",
        "new_location/my_file",
        this.table.locationProvider().newDataLocation("my_file")
    );
  }

  @Test
  public void testDynamicallyLoadedLocationProvider() {
    this.table.updateProperties()
        .set(TableProperties.LOCATION_PROVIDER_IMPL,
            String.format("%s$%s",
                this.getClass().getCanonicalName(),
                DynamicallyLoadedLocationProvider.class.getSimpleName()))
        .commit();

    Assert.assertTrue(String.format("Table should load impl defined in its properties"),
        this.table.locationProvider() instanceof DynamicallyLoadedLocationProvider
    );

    Assert.assertEquals(
        "Custom provider should take base table location",
        String.format("%s/test_custom_provider/%s", this.table.location(), "my_file"),
        this.table.locationProvider().newDataLocation("my_file")
    );
  }

  @Test
  public void testDynamicallyLoadedLocationProviderNotFound() {
    String nonExistentImpl = String.format("%s$NonExistent%s",
        this.getClass().getCanonicalName(),
        DynamicallyLoadedLocationProvider.class.getSimpleName());
    this.table.updateProperties()
        .set(TableProperties.LOCATION_PROVIDER_IMPL, nonExistentImpl)
        .commit();

    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage(
        String.format("Unable to instantiate provided implementation %s for %s.",
            nonExistentImpl,
            LocationProvider.class));
    this.table.locationProvider();
  }

  @Test
  public void testInvalidDynamicallyLoadedLocationProvider() {
    String invalidImpl = String.format("%s$%s",
        this.getClass().getCanonicalName(),
        InvalidDynamicallyLoadedLocationProvider.class.getSimpleName());
    this.table.updateProperties()
        .set(TableProperties.LOCATION_PROVIDER_IMPL, invalidImpl)
        .commit();

    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage(
        String.format("Unable to instantiate provided implementation %s for %s.",
            invalidImpl,
            LocationProvider.class));
    this.table.locationProvider();
  }
}
