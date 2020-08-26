/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.util;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.Objects;

@SuppressWarnings("checkstyle:HiddenField")
public class TestClock extends Clock {

  private Instant instant;
  private final ZoneId zone;

  public TestClock() {
    this(Instant.now(), ZoneId.systemDefault());
  }

  public TestClock(Instant instant, ZoneId zone) {
    this.instant = instant;
    this.zone = zone;
  }

  @Override public Clock withZone(ZoneId zone) {
    return new TestClock(instant, zone);
  }

  @Override public ZoneId getZone() {
    return zone;
  }

  @Override public Instant instant() {
    return instant;
  }

  public TestClock instant(Instant instant) {
    this.instant = instant;
    return this;
  }

  public TestClock plus(TemporalAmount amount) {
    instant = instant.plus(amount);
    return this;
  }

  public TestClock plus(long amountToAdd, TemporalUnit unit) {
    instant = instant.plus(amountToAdd, unit);
    return this;
  }

  public TestClock plusSeconds(long secondsToAdd) {
    instant = instant.plusSeconds(secondsToAdd);
    return this;
  }

  public TestClock plusMillis(long millisToAdd) {
    instant = instant.plusMillis(millisToAdd);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TestClock testClock = (TestClock) o;
    return Objects.equals(instant, testClock.instant) &&
        Objects.equals(zone, testClock.zone);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), instant, zone);
  }

  @Override
  public String toString() {
    return "TestClock{" +
        "instant=" + instant +
        ", zone=" + zone +
        '}';
  }
}
