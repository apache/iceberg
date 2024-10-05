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

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** Static registration and notification for listeners. */
public class Listeners {
  private Listeners() {}

  private static final Map<Class<?>, Queue<Listener<?>>> LISTENERS = Maps.newConcurrentMap();

  public static <E> void register(Listener<E> listener, Class<E> eventType) {
    Queue<Listener<?>> list =
        LISTENERS.computeIfAbsent(eventType, k -> new ConcurrentLinkedQueue<>());
    list.add(listener);
  }

  @SuppressWarnings("unchecked")
  public static <E> void notifyAll(E event) {
    Preconditions.checkNotNull(event, "Cannot notify listeners for a null event.");

    Queue<Listener<?>> list = LISTENERS.get(event.getClass());
    if (list != null) {
      for (Listener<?> value : list) {
        Listener<E> listener = (Listener<E>) value;
        listener.notify(event);
      }
    }
  }
}
