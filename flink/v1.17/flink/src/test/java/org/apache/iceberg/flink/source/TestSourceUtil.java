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
package org.apache.iceberg.flink.source;

import java.io.IOException;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.junit.Assert;
import org.junit.Test;

public class TestSourceUtil {
  @Test
  public void testInferedParallelism() throws IOException {
    Configuration configuration = new Configuration();
    // Empty table, infer parallelism should be at least 1
    int parallelism = SourceUtil.inferParallelism(configuration, -1L, () -> 0);
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);

    // 2 splits (max infer is the default value 100 , max > splits num), the parallelism is splits
    // num : 2
    parallelism = SourceUtil.inferParallelism(configuration, -1L, () -> 2);
    Assert.assertEquals("Should produce the expected parallelism.", 2, parallelism);

    // 2 splits and limit is 1 , max infer parallelism is default 100，
    // which is greater than splits num and limit, the parallelism is the limit value : 1
    parallelism = SourceUtil.inferParallelism(configuration, 1, () -> 2);
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);

    // 2 splits and max infer parallelism is 1 (max < splits num), the parallelism is  1
    configuration.setInteger(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX, 1);
    parallelism = SourceUtil.inferParallelism(configuration, -1L, () -> 2);
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);

    // 2 splits, max infer parallelism is 1, limit is 3, the parallelism is max infer parallelism :
    // 1
    parallelism = SourceUtil.inferParallelism(configuration, 3, () -> 2);
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);

    // 2 splits, infer parallelism is disabled, the parallelism is flink default parallelism 1
    configuration.setBoolean(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);
    parallelism = SourceUtil.inferParallelism(configuration, 3, () -> 2);
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);
  }
}
