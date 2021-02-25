/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iceberg.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;

public class JobGroupUtils {
    public static JobGroupInfo getCallSite(SparkContext sc) {
        String groupId = sc.getLocalProperty(SparkContext$.MODULE$.SPARK_JOB_GROUP_ID());
        String description = sc.getLocalProperty(SparkContext$.MODULE$.SPARK_JOB_DESCRIPTION());
        String interruptOnCancel = sc.getLocalProperty(SparkContext$.MODULE$.SPARK_JOB_INTERRUPT_ON_CANCEL());
        return new JobGroupInfo(groupId, description, interruptOnCancel);
    }

    public static void setCallSite(SparkContext sparkContext, JobGroupInfo obj) {
        sparkContext.setLocalProperty(SparkContext$.MODULE$.SPARK_JOB_DESCRIPTION(), obj.description);
        sparkContext.setLocalProperty(SparkContext$.MODULE$.SPARK_JOB_GROUP_ID(), obj.groupId);
        sparkContext.setLocalProperty(SparkContext$.MODULE$.SPARK_JOB_INTERRUPT_ON_CANCEL(), obj.interruptOnCancel);
    }
}
