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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestLongitudeBoundsBuilder {
  @Test
  void emptyInputHasNoBounds() {
    assertThat(new LongitudeBoundsBuilder().build()).isNull();
  }

  @ParameterizedTest
  @ValueSource(doubles = {-180.0, -45.0, 0.0, 45.0, 180.0})
  void preservesSinglePoint(double longitude) {
    LongitudeBoundsBuilder bounds = new LongitudeBoundsBuilder();
    bounds.add(longitude, longitude);

    assertBounds(bounds, longitude, longitude);
  }

  @Test
  void preservesBothStoredAntimeridianLongitudes() {
    LongitudeBoundsBuilder bounds = new LongitudeBoundsBuilder();
    bounds.add(-180.0, -180.0);
    bounds.add(180.0, 180.0);

    assertBounds(bounds, 180.0, -180.0);
  }

  @Test
  void mergesOverlappingTouchingAndContainedIntervals() {
    LongitudeBoundsBuilder bounds = new LongitudeBoundsBuilder();
    bounds.add(-40.0, -10.0);
    bounds.add(-20.0, 5.0);
    bounds.add(5.0, 20.0);
    bounds.add(-30.0, 10.0);

    assertBounds(bounds, -40.0, 20.0);
  }

  @Test
  void wrapsAcrossAntimeridian() {
    LongitudeBoundsBuilder bounds = new LongitudeBoundsBuilder();
    bounds.add(170.0, -170.0);

    assertBounds(bounds, 170.0, -170.0);
    assertContains(bounds.build(), -180.0, 180.0, 175.0, -175.0);
    assertThat(contains(bounds.build(), 0.0)).isFalse();
  }

  @Test
  void excludesLargestUncoveredGap() {
    LongitudeBoundsBuilder bounds = new LongitudeBoundsBuilder();
    bounds.add(-160.0, -150.0);
    bounds.add(20.0, 30.0);
    bounds.add(50.0, 70.0);

    assertBounds(bounds, 20.0, -150.0);
    assertContains(bounds.build(), -160.0, -150.0, 20.0, 30.0, 50.0, 70.0);
    assertThat(contains(bounds.build(), 0.0)).isFalse();
  }

  @Test
  void preservesCoveredIntervalsRatherThanOnlyTheirEndpoints() {
    LongitudeBoundsBuilder bounds = new LongitudeBoundsBuilder();
    bounds.add(100.0, -100.0);
    bounds.add(-10.0, 10.0);

    assertBounds(bounds, -10.0, -100.0);
    assertContains(bounds.build(), -180.0, 180.0, 100.0, -100.0, 0.0, 50.0);
    assertThat(contains(bounds.build(), -50.0)).isFalse();
  }

  @Test
  void preservesIntervalsWiderThanHalfACircle() {
    LongitudeBoundsBuilder bounds = new LongitudeBoundsBuilder();
    bounds.add(-150.0, 150.0);

    assertBounds(bounds, -150.0, 150.0);
    assertContains(bounds.build(), -150.0, 0.0, 150.0);
  }

  @Test
  void fullRangeRemainsFullAfterMoreInput() {
    LongitudeBoundsBuilder bounds = new LongitudeBoundsBuilder();
    bounds.add(-180.0, 180.0);
    bounds.add(170.0, -170.0);
    bounds.add(0.0, 0.0);

    assertBounds(bounds, -180.0, 180.0);
  }

  @Test
  void mergedCoverageCanFillTheCircle() {
    LongitudeBoundsBuilder bounds = new LongitudeBoundsBuilder();
    bounds.add(-180.0, -10.0);
    bounds.add(-10.0, 180.0);

    assertBounds(bounds, -180.0, 180.0);
  }

  @Test
  void repeatedBuildDoesNotDiscardCoverage() {
    LongitudeBoundsBuilder bounds = new LongitudeBoundsBuilder();
    bounds.add(-30.0, -20.0);
    assertBounds(bounds, -30.0, -20.0);
    assertBounds(bounds, -30.0, -20.0);
    bounds.add(20.0, 30.0);

    assertBounds(bounds, -30.0, 30.0);
  }

  @Test
  void keepsCoverageAcrossManyDisjointIntervalsAndLaterInput() {
    LongitudeBoundsBuilder bounds = new LongitudeBoundsBuilder();
    for (int index = 0; index < 256; index += 1) {
      double west = -179.75 + 1.25 * index;
      bounds.add(west, west + 0.25);
    }

    bounds.add(165.0, 175.0);
    LongitudeBoundsBuilder.Interval interval = bounds.build();
    for (int index = 0; index < 256; index += 1) {
      double west = -179.75 + 1.25 * index;
      assertContains(interval, west, west + 0.125, west + 0.25);
    }

    assertContains(interval, 165.0, 170.0, 175.0);
  }

  private static void assertBounds(LongitudeBoundsBuilder bounds, double west, double east) {
    LongitudeBoundsBuilder.Interval interval = bounds.build();
    assertThat(interval).isNotNull();
    assertThat(interval.west()).isEqualTo(west);
    assertThat(interval.east()).isEqualTo(east);
  }

  private static void assertContains(
      LongitudeBoundsBuilder.Interval interval, double... longitudes) {
    for (double longitude : longitudes) {
      assertThat(contains(interval, longitude)).as("contains longitude %s", longitude).isTrue();
    }
  }

  private static boolean contains(LongitudeBoundsBuilder.Interval interval, double longitude) {
    return interval.west() <= interval.east()
        ? longitude >= interval.west() && longitude <= interval.east()
        : longitude >= interval.west() || longitude <= interval.east();
  }
}
