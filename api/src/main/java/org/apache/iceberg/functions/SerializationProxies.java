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
package org.apache.iceberg.functions;

import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * Stand-in classes for function classes in Java Serialization.
 *
 * <p>These are used so that function classes can be singletons and use identical equality.
 */
class SerializationProxies {
  private SerializationProxies() {}

  static class MaskAlphanumProxy implements Serializable {
    private static final MaskAlphanumProxy INSTANCE = new MaskAlphanumProxy();

    static MaskAlphanumProxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    MaskAlphanumProxy() {}

    Object readResolve() throws ObjectStreamException {
      return MaskAlphanum.get();
    }
  }

  static class MaskToFixedValueProxy implements Serializable {
    private static final MaskToFixedValueProxy INSTANCE = new MaskToFixedValueProxy();

    static MaskToFixedValueProxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    MaskToFixedValueProxy() {}

    Object readResolve() throws ObjectStreamException {
      return MaskToFixedValue.get();
    }
  }

  static class ReplaceWithNullProxy implements Serializable {
    private static final ReplaceWithNullProxy INSTANCE = new ReplaceWithNullProxy();

    static ReplaceWithNullProxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    ReplaceWithNullProxy() {}

    Object readResolve() throws ObjectStreamException {
      return ReplaceWithNull.get();
    }
  }

  static class ShowFirst4Proxy implements Serializable {
    private static final ShowFirst4Proxy INSTANCE = new ShowFirst4Proxy();

    static ShowFirst4Proxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    ShowFirst4Proxy() {}

    Object readResolve() throws ObjectStreamException {
      return ShowFirst4.get();
    }
  }

  static class ShowLast4Proxy implements Serializable {
    private static final ShowLast4Proxy INSTANCE = new ShowLast4Proxy();

    static ShowLast4Proxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    ShowLast4Proxy() {}

    Object readResolve() throws ObjectStreamException {
      return ShowLast4.get();
    }
  }

  static class TruncateToYearProxy implements Serializable {
    private static final TruncateToYearProxy INSTANCE = new TruncateToYearProxy();

    static TruncateToYearProxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    TruncateToYearProxy() {}

    Object readResolve() throws ObjectStreamException {
      return TruncateToYear.get();
    }
  }

  static class TruncateToMonthProxy implements Serializable {
    private static final TruncateToMonthProxy INSTANCE = new TruncateToMonthProxy();

    static TruncateToMonthProxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    TruncateToMonthProxy() {}

    Object readResolve() throws ObjectStreamException {
      return TruncateToMonth.get();
    }
  }

  static class Sha256GlobalProxy implements Serializable {
    private static final Sha256GlobalProxy INSTANCE = new Sha256GlobalProxy();

    static Sha256GlobalProxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    Sha256GlobalProxy() {}

    Object readResolve() throws ObjectStreamException {
      return Sha256Global.get();
    }
  }

  static class Sha256QueryLocalProxy implements Serializable {
    private static final Sha256QueryLocalProxy INSTANCE = new Sha256QueryLocalProxy();

    static Sha256QueryLocalProxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    Sha256QueryLocalProxy() {}

    Object readResolve() throws ObjectStreamException {
      return Sha256QueryLocal.get();
    }
  }
}
