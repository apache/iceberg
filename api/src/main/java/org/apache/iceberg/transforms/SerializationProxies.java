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
package org.apache.iceberg.transforms;

import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * Stand-in classes for expression classes in Java Serialization.
 *
 * <p>These are used so that transform classes can be singletons and use identical equality.
 */
class SerializationProxies {
  private SerializationProxies() {}

  static class VoidTransformProxy implements Serializable {
    private static final VoidTransformProxy INSTANCE = new VoidTransformProxy();

    static VoidTransformProxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    VoidTransformProxy() {}

    Object readResolve() throws ObjectStreamException {
      return VoidTransform.get();
    }
  }

  static class IdentityTransformProxy implements Serializable {
    private static final IdentityTransformProxy INSTANCE = new IdentityTransformProxy();

    static IdentityTransformProxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    IdentityTransformProxy() {}

    Object readResolve() throws ObjectStreamException {
      return Identity.get();
    }
  }

  static class YearsTransformProxy implements Serializable {
    private static final YearsTransformProxy INSTANCE = new YearsTransformProxy();

    static YearsTransformProxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    YearsTransformProxy() {}

    Object readResolve() throws ObjectStreamException {
      return Years.get();
    }
  }

  static class MonthsTransformProxy implements Serializable {
    private static final MonthsTransformProxy INSTANCE = new MonthsTransformProxy();

    static MonthsTransformProxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    MonthsTransformProxy() {}

    Object readResolve() throws ObjectStreamException {
      return Months.get();
    }
  }

  static class DaysTransformProxy implements Serializable {
    private static final DaysTransformProxy INSTANCE = new DaysTransformProxy();

    static DaysTransformProxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    DaysTransformProxy() {}

    Object readResolve() throws ObjectStreamException {
      return Days.get();
    }
  }

  static class HoursTransformProxy implements Serializable {
    private static final HoursTransformProxy INSTANCE = new HoursTransformProxy();

    static HoursTransformProxy get() {
      return INSTANCE;
    }

    /** Constructor for Java serialization. */
    HoursTransformProxy() {}

    Object readResolve() throws ObjectStreamException {
      return Hours.get();
    }
  }
}
