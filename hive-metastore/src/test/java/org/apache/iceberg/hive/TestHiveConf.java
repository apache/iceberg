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

package org.apache.iceberg.hive;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Test;

/**
 * For https://github.com/apache/iceberg/issues/2070
 */
public class TestHiveConf {
    private static final String HIVE_SITE_CONTENT = "<?xml version=\"1.0\"?>\n" +
            "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n" +
            "<configuration>\n" +
            "  <property>\n" +
            "    <name>hive.metastore.sasl.enabled</name>\n" +
            "    <value>true</value>\n" +
            "  </property>\n" +
            "  <property>\n" +
            "    <name>hive.metastore.uris</name>\n" +
            "    <value>thrift://xxxx:1234</value>\n" +
            "  </property>\n" +
            "</configuration>\n";

    @Test
    public void test() {
        HiveConf hiveConf = createHiveConf();
        Assert.assertTrue(hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL));
        Assert.assertEquals("thrift://xxxx:1234", hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));

        // will override configurations from `hiveConf` with hive default values which default value is null or empty string
        HiveConf hiveConfNew = new HiveConf(hiveConf, HiveConf.class);
        Assert.assertFalse(hiveConfNew.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL));
        Assert.assertEquals("thrift://xxxx:1234", hiveConfNew.getVar(HiveConf.ConfVars.METASTOREURIS));

        hiveConfNew.addResource(hiveConf);
        Assert.assertTrue(hiveConfNew.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL));
        Assert.assertEquals("thrift://xxxx:1234", hiveConfNew.getVar(HiveConf.ConfVars.METASTOREURIS));
    }

    private HiveConf createHiveConf() {
        HiveConf hiveConf = new HiveConf();
        try (InputStream inputStream = new ByteArrayInputStream(HIVE_SITE_CONTENT.getBytes(StandardCharsets.UTF_8))) {
            hiveConf.addResource(inputStream, "for_test");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return hiveConf;
    }
}
