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
package org.apache.iceberg.vortex;

import dev.vortex.api.Session;

/**
 * Provides the process-wide shared Vortex {@link Session}.
 *
 * <p>Creating a session builds the full native encoding/kernel registries and is expensive, so a
 * single session is shared by all readers and writers in the JVM (sessions are documented as safe
 * to share across threads). The session lives for the lifetime of the process; its native resources
 * are reclaimed on JVM exit.
 */
public final class VortexSessions {

  private VortexSessions() {}

  private static final class Holder {
    private static final Session SHARED = Session.create();
  }

  public static Session shared() {
    return Holder.SHARED;
  }
}
