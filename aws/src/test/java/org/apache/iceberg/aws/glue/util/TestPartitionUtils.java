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

package org.apache.iceberg.aws.glue.util;

import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.InternalServiceException;
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.ResourceNumberLimitExceededException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestPartitionUtils {

  @Test
  public void testBuildPartitionMapAndGetPartitionValuesList() {
    String namespaceName = "ns";
    String tableName = "table";
    // choose special values to make values1.hashCode() == values2.hashCode()
    List<String> values1 = Lists.newArrayList("Aa");
    List<String> values2 = Lists.newArrayList("BB");
    Partition partition1 = ObjectTestUtils.getTestPartition(namespaceName, tableName, values1);
    Partition partition2 = ObjectTestUtils.getTestPartition(namespaceName, tableName, values2);
    Map<PartitionKey, Partition> partitionMap = PartitionUtils.buildPartitionMap(
        Lists.newArrayList(partition1, partition2));
    List<PartitionValueList> partitionValuesList = PartitionUtils.getPartitionValuesList(partitionMap);

    assertEquals(2, partitionMap.size());
    Set<List<String>> valuesSet = Sets.newHashSet(values1, values2);
    for (PartitionKey partitionKey : partitionMap.keySet()) {
      assertThat(valuesSet, hasItem(partitionKey.getValues()));
      assertThat(partitionMap.get(partitionKey).getValues(), equalTo(partitionKey.getValues()));
    }

    assertEquals(2, partitionValuesList.size());
    for (PartitionValueList partitionValueList : partitionValuesList) {
      assertThat(valuesSet, hasItem(partitionValueList.getValues()));
    }
  }

  @Test
  public void testIsInvalidUserInputException() {
    assertTrue(PartitionUtils.isInvalidUserInputException(new InvalidInputException("foo")));
    assertTrue(PartitionUtils.isInvalidUserInputException(new EntityNotFoundException("bar")));
    assertFalse(PartitionUtils.isInvalidUserInputException(new InternalServiceException("bar2")));
    assertFalse(PartitionUtils.isInvalidUserInputException(new ResourceNumberLimitExceededException("bar3")));
    assertFalse(PartitionUtils.isInvalidUserInputException(new NullPointerException("bar4")));
  }

}
