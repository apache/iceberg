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
package org.apache.iceberg.rest.auth;

import org.junit.Assert;
import org.junit.Test;

public class TestOAuth2Util {
  @Test
  public void testOAuthScopeTokenValidation() {
    // valid scope tokens are from ascii 0x21 to 0x7E, excluding 0x22 (") and 0x5C (\)
    // test characters that are outside of the ! to ~ range and the excluded characters, " and \
    Assert.assertFalse("Should reject scope token with \\", OAuth2Util.isValidScopeToken("a\\b"));
    Assert.assertFalse("Should reject scope token with space", OAuth2Util.isValidScopeToken("a b"));
    Assert.assertFalse("Should reject scope token with \"", OAuth2Util.isValidScopeToken("a\"b"));
    Assert.assertFalse(
        "Should reject scope token with DEL", OAuth2Util.isValidScopeToken("\u007F"));
    // test all characters that are inside of the ! to ~ range and are not excluded
    Assert.assertTrue(
        "Should accept scope token with !-/", OAuth2Util.isValidScopeToken("!#$%&'()*+,-./"));
    Assert.assertTrue(
        "Should accept scope token with 0-9", OAuth2Util.isValidScopeToken("0123456789"));
    Assert.assertTrue(
        "Should accept scope token with :-@", OAuth2Util.isValidScopeToken(":;<=>?@"));
    Assert.assertTrue(
        "Should accept scope token with A-M", OAuth2Util.isValidScopeToken("ABCDEFGHIJKLM"));
    Assert.assertTrue(
        "Should accept scope token with N-Z", OAuth2Util.isValidScopeToken("NOPQRSTUVWXYZ"));
    Assert.assertTrue(
        "Should accept scope token with [-`, not \\", OAuth2Util.isValidScopeToken("[]^_`"));
    Assert.assertTrue(
        "Should accept scope token with a-m", OAuth2Util.isValidScopeToken("abcdefghijklm"));
    Assert.assertTrue(
        "Should accept scope token with n-z", OAuth2Util.isValidScopeToken("nopqrstuvwxyz"));
    Assert.assertTrue("Should accept scope token with {-~", OAuth2Util.isValidScopeToken("{|}~"));
  }
}
