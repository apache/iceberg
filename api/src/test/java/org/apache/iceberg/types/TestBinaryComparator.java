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

import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.iceberg.expressions.Literal;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the comparator returned by binary and fixed literals.
 *
 * <p>The tests use assertTrue instead of assertEquals because the return value is not necessarily
 * one of {-1, 0, 1}. It is also more clear to compare the return value to 0 because the same
 * operation can be used: a &lt; b is equivalent to compare(a, b) &lt; 0.
 */
public class TestBinaryComparator {
  @Test
  public void testBinaryUnsignedComparator() {
    // b1 < b2 because comparison is unsigned, and -1 has msb set
    ByteBuffer b1 = ByteBuffer.wrap(new byte[] {1, 1, 2});
    ByteBuffer b2 = ByteBuffer.wrap(new byte[] {1, -1, 2});

    Comparator<ByteBuffer> cmp = Literal.of(b1).comparator();

    Assert.assertTrue("Negative bytes should sort after positive bytes", cmp.compare(b1, b2) < 0);
  }

  @Test
  public void testFixedUnsignedComparator() {
    // b1 < b2 because comparison is unsigned, and -1 has msb set
    ByteBuffer b1 = ByteBuffer.wrap(new byte[] {1, 1, 2});
    ByteBuffer b2 = ByteBuffer.wrap(new byte[] {1, -1, 2});

    Literal<ByteBuffer> fixedLit = Literal.of(b1).to(Types.FixedType.ofLength(3));
    Comparator<ByteBuffer> cmp = fixedLit.comparator();

    Assert.assertTrue("Negative bytes should sort after positive bytes", cmp.compare(b1, b2) < 0);
  }

  @Test
  public void testNullHandling() {
    ByteBuffer buf = ByteBuffer.allocate(0);

    Comparator<ByteBuffer> cmp = Literal.of(buf).comparator();
    Assert.assertTrue("null comes before non-null", cmp.compare(null, buf) < 0);
    Assert.assertTrue("null comes before non-null", cmp.compare(buf, null) > 0);
    Assert.assertEquals("null equals null", 0, cmp.compare(null, null));
  }
}
