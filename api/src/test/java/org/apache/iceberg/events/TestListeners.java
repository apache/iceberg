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
package org.apache.iceberg.events;

import org.junit.Assert;
import org.junit.Test;

public class TestListeners {
  static {
    Listeners.register(TestListener.get()::event1, Event1.class);
    Listeners.register(TestListener.get()::event2, Event2.class);
  }

  public static class Event1 {}

  public static class Event2 {}

  public static class TestListener {
    private static final TestListener INSTANCE = new TestListener();

    public static TestListener get() {
      return INSTANCE;
    }

    private Event1 e1 = null;
    private Event2 e2 = null;

    public void event1(Event1 event) {
      this.e1 = event;
    }

    public void event2(Event2 event) {
      this.e2 = event;
    }
  }

  @Test
  public void testEvent1() {
    Event1 e1 = new Event1();

    Listeners.notifyAll(e1);

    Assert.assertEquals(e1, TestListener.get().e1);
  }

  @Test
  public void testEvent2() {
    Event2 e2 = new Event2();

    Listeners.notifyAll(e2);

    Assert.assertEquals(e2, TestListener.get().e2);
  }

  @Test
  public void testMultipleListeners() {
    TestListener other = new TestListener();
    Listeners.register(other::event1, Event1.class);

    Event1 e1 = new Event1();

    Listeners.notifyAll(e1);

    Assert.assertEquals(e1, TestListener.get().e1);
    Assert.assertEquals(e1, other.e1);
  }
}
