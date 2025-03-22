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
package org.apache.iceberg.types;

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** The algorithm for interpolating edges. */
public enum EdgeAlgorithm {
  /** Edges are interpolated as geodesics on a sphere. */
  SPHERICAL,
  /** See <a href="https://en.wikipedia.org/wiki/Vincenty%27s_formulae">Vincenty's formulae</a> */
  VINCENTY,
  /**
   * Thomas, Paul D. Spheroidal geodesics, reference systems, &amp; local geometry. US Naval
   * Oceanographic Office, 1970.
   */
  THOMAS,
  /**
   * Thomas, Paul D. Mathematical models for navigation systems. US Naval Oceanographic Office,
   * 1965.
   */
  ANDOYER,
  /**
   * <a href="https://link.springer.com/content/pdf/10.1007/s00190-012-0578-z.pdf">Karney, Charles
   * FF. "Algorithms for geodesics." Journal of Geodesy 87 (2013): 43-55 </a>, and <a
   * href="https://geographiclib.sourceforge.io/">GeographicLib</a>.
   */
  KARNEY;

  public static EdgeAlgorithm fromName(String algorithmName) {
    Preconditions.checkNotNull(algorithmName, "Invalid edge interpolation algorithm: null");
    try {
      return EdgeAlgorithm.valueOf(algorithmName.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Invalid edge interpolation algorithm: %s", algorithmName), e);
    }
  }

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ENGLISH);
  }
}
