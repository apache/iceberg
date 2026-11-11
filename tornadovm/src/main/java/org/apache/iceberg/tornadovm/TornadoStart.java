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
package org.apache.iceberg.tornadovm;

import uk.ac.manchester.tornado.api.runtime.TornadoRuntimeProvider;

/**
 * Starts TornadoVM's runtime with TornadoVM's own class loader as the thread's context class
 * loader.
 *
 * <p>The runtime looks its backends up through the context class loader of the thread that first
 * uses it, once. A Spark 4 task thread's loader does not see TornadoVM's modules: started there,
 * the runtime would have no backend for the rest of the JVM's life.
 */
final class TornadoStart {
  private TornadoStart() {}

  static void ensureStarted() {
    Holder.touch();
  }

  private static final class Holder {
    static {
      Thread thread = Thread.currentThread();
      ClassLoader previous = thread.getContextClassLoader();
      thread.setContextClassLoader(TornadoRuntimeProvider.class.getClassLoader());
      try {
        TornadoRuntimeProvider.getTornadoRuntime();
      } catch (RuntimeException | LinkageError e) {
        // No TornadoVM runtime in this JVM (the CPU twin, a plain JVM): device work fails when
        // tried.
      } finally {
        thread.setContextClassLoader(previous);
      }
    }

    private Holder() {}

    static void touch() {}
  }
}
