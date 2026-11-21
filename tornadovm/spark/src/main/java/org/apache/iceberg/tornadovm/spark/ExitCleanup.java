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
package org.apache.iceberg.tornadovm.spark;

/**
 * Freeing the device at exit. Spark gives an executor no lifecycle hook for a cache of device
 * scans, so they are closed from a shutdown hook -- bounded in time, and safe to register while the
 * JVM is already shutting down.
 */
final class ExitCleanup {
  private ExitCleanup() {}

  /** Runs {@code close} at exit, for at most 10 seconds; nothing if the JVM is already exiting. */
  @SuppressWarnings("ShutdownHook")
  static void register(Runnable close) {
    try {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> within(close), "tornadovm-exit"));
    } catch (IllegalStateException alreadyExiting) {
      // The JVM is exiting already: there is nothing left to free the device for.
    }
  }

  /**
   * Runs {@code close} for at most 10 seconds: freeing the device must not keep a JVM from exiting
   * -- a scan stuck on a broken device would hold it, and the host memory, forever.
   */
  private static void within(Runnable close) {
    Thread closing = new Thread(close, "tornadovm-close");
    closing.setDaemon(true);
    closing.start();
    try {
      closing.join(10_000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
