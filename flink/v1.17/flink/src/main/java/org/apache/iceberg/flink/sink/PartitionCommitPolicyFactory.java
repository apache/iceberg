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
package org.apache.iceberg.flink.sink;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.annotation.Internal;

/** A factory to create {@link PartitionCommitPolicy} chain. */
@Internal
public class PartitionCommitPolicyFactory implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String policyKind;
  private final String customClass;
  private final String successFileName;

  public PartitionCommitPolicyFactory(
      String policyKind, String customClass, String successFileName) {
    this.policyKind = policyKind;
    this.customClass = customClass;
    this.successFileName = successFileName;
  }

  /** Create a policy chain. */
  public List<PartitionCommitPolicy> createPolicyChain(ClassLoader cl) {
    if (policyKind == null) {
      return Collections.emptyList();
    }
    String[] policyStrings = policyKind.split(",");
    return Arrays.stream(policyStrings)
        .filter(name -> !name.equalsIgnoreCase(PartitionCommitPolicy.DEFAULT))
        .map(
            name -> {
              switch (name.toLowerCase()) {
                case PartitionCommitPolicy.SUCCESS_FILE:
                  return new SuccessFileCommitPolicy(successFileName);
                case PartitionCommitPolicy.CUSTOM:
                  try {
                    return (PartitionCommitPolicy)
                        cl.loadClass(customClass).getDeclaredConstructor().newInstance();
                  } catch (ClassNotFoundException
                      | IllegalAccessException
                      | InstantiationException
                      | NoSuchMethodException
                      | InvocationTargetException e) {
                    throw new RuntimeException(
                        "Can not create new instance for custom class from " + customClass, e);
                  }
                default:
                  throw new UnsupportedOperationException("Unsupported policy: " + name);
              }
            })
        .collect(Collectors.toList());
  }
}
