/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.mr.hive.vector;

import java.sql.Date;
import java.sql.Timestamp;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains ported code snippets from later Hive sources. We should get rid of this class as soon as Hive 4 is released
 * and Iceberg makes a dependency to that version.
 */
public class CompatibilityHiveVectorUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CompatibilityHiveVectorUtils.class);

  private CompatibilityHiveVectorUtils() {

  }

  /**
   * Returns serialized mapwork instance from a job conf - ported from Hive source code LlapHiveUtils#findMapWork
   *
   * @param job JobConf instance
   * @return
   */
  @SuppressWarnings("Slf4jConstantLogMessage")
  public static MapWork findMapWork(JobConf job) {
    String inputName = job.get(Utilities.INPUT_NAME, null);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing for input " + inputName);
    }
    String prefixes = job.get(DagUtils.TEZ_MERGE_WORK_FILE_PREFIXES);
    if (prefixes != null && !StringUtils.isBlank(prefixes)) {
      // Currently SMB is broken, so we cannot check if it's  compatible with IO elevator.
      // So, we don't use the below code that would get the correct MapWork. See HIVE-16985.
      return null;
    }

    BaseWork work = null;
    // HIVE-16985: try to find the fake merge work for SMB join, that is really another MapWork.
    if (inputName != null) {
      if (prefixes == null || !Lists.newArrayList(prefixes.split(",")).contains(inputName)) {
        inputName = null;
      }
    }
    if (inputName != null) {
      work = Utilities.getMergeWork(job, inputName);
    }

    if (!(work instanceof MapWork)) {
      work = Utilities.getMapWork(job);
    }
    return (MapWork) work;
  }


  /**
   * Ported from Hive source code VectorizedRowBatchCtx#addPartitionColsToBatch
   *
   * @param col ColumnVector to write the partition value into
   * @param value partition value
   * @param partitionColumnName partition key
   * @param rowColumnTypeInfo column type description
   */
  @SuppressWarnings({"AvoidNestedBlocks", "FallThrough", "MethodLength", "CyclomaticComplexity", "Indentation"})
  public static void addPartitionColsToBatch(ColumnVector col, Object value, String partitionColumnName,
      TypeInfo rowColumnTypeInfo) {
    PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) rowColumnTypeInfo;
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
      case BOOLEAN: {
        LongColumnVector lcv = (LongColumnVector) col;
        if (value == null) {
          lcv.noNulls = false;
          lcv.isNull[0] = true;
          lcv.isRepeating = true;
        } else {
          lcv.fill((Boolean) value ? 1 : 0);
          lcv.isNull[0] = false;
        }
      }
      break;

      case BYTE: {
        LongColumnVector lcv = (LongColumnVector) col;
        if (value == null) {
          lcv.noNulls = false;
          lcv.isNull[0] = true;
          lcv.isRepeating = true;
        } else {
          lcv.fill((Byte) value);
          lcv.isNull[0] = false;
        }
      }
      break;

      case SHORT: {
        LongColumnVector lcv = (LongColumnVector) col;
        if (value == null) {
          lcv.noNulls = false;
          lcv.isNull[0] = true;
          lcv.isRepeating = true;
        } else {
          lcv.fill((Short) value);
          lcv.isNull[0] = false;
        }
      }
      break;

      case INT: {
        LongColumnVector lcv = (LongColumnVector) col;
        if (value == null) {
          lcv.noNulls = false;
          lcv.isNull[0] = true;
          lcv.isRepeating = true;
        } else {
          lcv.fill((Integer) value);
          lcv.isNull[0] = false;
        }
      }
      break;

      case LONG: {
        LongColumnVector lcv = (LongColumnVector) col;
        if (value == null) {
          lcv.noNulls = false;
          lcv.isNull[0] = true;
          lcv.isRepeating = true;
        } else {
          lcv.fill((Long) value);
          lcv.isNull[0] = false;
        }
      }
      break;

      case DATE: {
        LongColumnVector lcv = (LongColumnVector) col;
        if (value == null) {
          lcv.noNulls = false;
          lcv.isNull[0] = true;
          lcv.isRepeating = true;
        } else {
          lcv.fill(DateWritable.dateToDays((Date) value));
          lcv.isNull[0] = false;
        }
      }
      break;

      case TIMESTAMP: {
        TimestampColumnVector lcv = (TimestampColumnVector) col;
        if (value == null) {
          lcv.noNulls = false;
          lcv.isNull[0] = true;
          lcv.isRepeating = true;
        } else {
          lcv.fill((Timestamp) value);
          lcv.isNull[0] = false;
        }
      }
      break;

      case INTERVAL_YEAR_MONTH: {
        LongColumnVector lcv = (LongColumnVector) col;
        if (value == null) {
          lcv.noNulls = false;
          lcv.isNull[0] = true;
          lcv.isRepeating = true;
        } else {
          lcv.fill(((HiveIntervalYearMonth) value).getTotalMonths());
          lcv.isNull[0] = false;
        }
      }

      case INTERVAL_DAY_TIME: {
        IntervalDayTimeColumnVector icv = (IntervalDayTimeColumnVector) col;
        if (value == null) {
          icv.noNulls = false;
          icv.isNull[0] = true;
          icv.isRepeating = true;
        } else {
          icv.fill((HiveIntervalDayTime) value);
          icv.isNull[0] = false;
        }
      }

      case FLOAT: {
        DoubleColumnVector dcv = (DoubleColumnVector) col;
        if (value == null) {
          dcv.noNulls = false;
          dcv.isNull[0] = true;
          dcv.isRepeating = true;
        } else {
          dcv.fill((Float) value);
          dcv.isNull[0] = false;
        }
      }
      break;

      case DOUBLE: {
        DoubleColumnVector dcv = (DoubleColumnVector) col;
        if (value == null) {
          dcv.noNulls = false;
          dcv.isNull[0] = true;
          dcv.isRepeating = true;
        } else {
          dcv.fill((Double) value);
          dcv.isNull[0] = false;
        }
      }
      break;

      case DECIMAL: {
        DecimalColumnVector dv = (DecimalColumnVector) col;
        if (value == null) {
          dv.noNulls = false;
          dv.isNull[0] = true;
          dv.isRepeating = true;
        } else {
          HiveDecimal hd = (HiveDecimal) value;
          dv.set(0, hd);
          dv.isRepeating = true;
          dv.isNull[0] = false;
        }
      }
      break;

      case BINARY: {
        BytesColumnVector bcv = (BytesColumnVector) col;
        byte[] bytes = (byte[]) value;
        if (bytes == null) {
          bcv.noNulls = false;
          bcv.isNull[0] = true;
          bcv.isRepeating = true;
        } else {
          bcv.fill(bytes);
          bcv.isNull[0] = false;
        }
      }
      break;

      case STRING:
      case CHAR:
      case VARCHAR: {
        BytesColumnVector bcv = (BytesColumnVector) col;
        String sVal = value.toString();
        if (sVal == null) {
          bcv.noNulls = false;
          bcv.isNull[0] = true;
          bcv.isRepeating = true;
        } else {
          bcv.setVal(0, sVal.getBytes());
          bcv.isRepeating = true;
        }
      }
      break;

      default:
        throw new RuntimeException("Unable to recognize the partition type " +
            primitiveTypeInfo.getPrimitiveCategory() + " for column " + partitionColumnName);
    }

  }
}
