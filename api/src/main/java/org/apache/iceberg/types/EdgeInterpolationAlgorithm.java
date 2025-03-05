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

/** The algorithm for interpolating edges. */
public enum EdgeInterpolationAlgorithm {
  /** Edges are interpolated as geodesics on a sphere. */
  SPHERICAL("spherical"),
  /** See <a href="https://en.wikipedia.org/wiki/Vincenty%27s_formulae">Vincenty's formulae</a> */
  VINCENTY("vincenty"),
  /**
   * Thomas, Paul D. Spheroidal geodesics, reference systems, &amp; local geometry. US Naval
   * Oceanographic Office, 1970.
   */
  THOMAS("thomas"),
  /**
   * Thomas, Paul D. Mathematical models for navigation systems. US Naval Oceanographic Office,
   * 1965.
   */
  ANDOYER("andoyer"),
  /**
   * <a href="https://link.springer.com/content/pdf/10.1007/s00190-012-0578-z.pdf">Karney, Charles
   * FF. "Algorithms for geodesics." Journal of Geodesy 87 (2013): 43-55 </a>, and <a
   * href="https://geographiclib.sourceforge.io/">GeographicLib</a>.
   */
  KARNEY("karney");

  private final String value;

  EdgeInterpolationAlgorithm(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }

  public static EdgeInterpolationAlgorithm fromName(String algorithmName) {
    try {
      return EdgeInterpolationAlgorithm.valueOf(algorithmName.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Invalid edge interpolation algorithm name: %s", algorithmName), e);
    }
  }
}
