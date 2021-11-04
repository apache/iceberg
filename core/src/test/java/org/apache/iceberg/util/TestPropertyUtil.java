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

package org.apache.iceberg.util;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class TestPropertyUtil {

  @Test
  public void testResolveLong2LevelOptionExists() {
    Map<String, String> options = ImmutableMap.of("option-key", "1");
    Map<String, String> properties = ImmutableMap.of("property-key", "2");
    Assert.assertEquals("Option value should be used if exists", 1L,
        PropertyUtil.resolveLongProperty(options, "option-key", properties, "property-key", 3L));
  }

  @Test
  public void testResolveLong2LevelPropertyExists() {
    Map<String, String> options = ImmutableMap.of();
    Map<String, String> properties = ImmutableMap.of("property-key", "2");
    Assert.assertEquals("Property value should be used if exists", 2L,
        PropertyUtil.resolveLongProperty(options, "option-key", properties, "property-key", 3L));
  }

  @Test
  public void testResolveLong2LevelDefault() {
    Map<String, String> options = ImmutableMap.of();
    Map<String, String> properties = ImmutableMap.of();
    Assert.assertEquals("Default should be used if no override exists", 3L,
        PropertyUtil.resolveLongProperty(options, "option-key", properties, "property-key", 3L));
  }

  @Test
  public void testResolveLong3LevelOptionExists() {
    Map<String, String> options = ImmutableMap.of("option-key", "1");
    Map<String, String> properties = ImmutableMap.of("property-key", "2");
    Map<String, String> properties2 = ImmutableMap.of("property-key2", "3");
    Assert.assertEquals("Option value should be used if exists", 1L,
        PropertyUtil.resolveLongProperty(options, "option-key",
            properties, "property-key", properties2, "property-key2", 4L));
  }

  @Test
  public void testResolveLong3LevelProperty1Exists() {
    Map<String, String> options = ImmutableMap.of();
    Map<String, String> properties = ImmutableMap.of("property-key", "2");
    Map<String, String> properties2 = ImmutableMap.of("property-key2", "3");
    Assert.assertEquals("Property1 value should be used if exists", 2L,
        PropertyUtil.resolveLongProperty(options, "option-key",
            properties, "property-key", properties2, "property-key2", 4L));
  }

  @Test
  public void testResolveLong3LevelProperty2Exists() {
    Map<String, String> options = ImmutableMap.of();
    Map<String, String> properties = ImmutableMap.of();
    Map<String, String> properties2 = ImmutableMap.of("property-key2", "3");
    Assert.assertEquals("Property2 value should be used if exists", 3L,
        PropertyUtil.resolveLongProperty(options, "option-key",
            properties, "property-key", properties2, "property-key2", 4L));
  }

  @Test
  public void testResolveLong3LevelDefault() {
    Map<String, String> options = ImmutableMap.of();
    Map<String, String> properties = ImmutableMap.of();
    Map<String, String> properties2 = ImmutableMap.of();
    Assert.assertEquals("Default should be used if no override exists", 4L,
        PropertyUtil.resolveLongProperty(options, "option-key",
            properties, "property-key", properties2, "property-key2", 4L));
  }

  @Test
  public void testResolveInt2LevelOptionExists() {
    Map<String, String> options = ImmutableMap.of("option-key", "1");
    Map<String, String> properties = ImmutableMap.of("property-key", "2");
    Assert.assertEquals("Option value should be used if exists", 1,
        PropertyUtil.resolveIntProperty(options, "option-key", properties, "property-key", 3));
  }

  @Test
  public void testResolveInt2LevelPropertyExists() {
    Map<String, String> options = ImmutableMap.of();
    Map<String, String> properties = ImmutableMap.of("property-key", "2");
    Assert.assertEquals("Property value should be used if exists", 2,
        PropertyUtil.resolveIntProperty(options, "option-key", properties, "property-key", 3));
  }

  @Test
  public void testResolveInt2LevelDefault() {
    Map<String, String> options = ImmutableMap.of();
    Map<String, String> properties = ImmutableMap.of();
    Assert.assertEquals("Default should be used if no override exists", 3,
        PropertyUtil.resolveIntProperty(options, "option-key", properties, "property-key", 3));
  }

  @Test
  public void testResolveBoolean2LevelOptionExists() {
    Map<String, String> options = ImmutableMap.of("option-key", "true");
    Map<String, String> properties = ImmutableMap.of("property-key", "false");
    Assert.assertTrue("Option value should be used if exists",
        PropertyUtil.resolveBooleanProperty(options, "option-key", properties, "property-key", false));
  }

  @Test
  public void testResolveBoolean2LevelPropertyExists() {
    Map<String, String> options = ImmutableMap.of();
    Map<String, String> properties = ImmutableMap.of("property-key", "true");
    Assert.assertTrue("Property value should be used if exists",
        PropertyUtil.resolveBooleanProperty(options, "option-key", properties, "property-key", false));
  }

  @Test
  public void testResolveBoolean2LevelDefault() {
    Map<String, String> options = ImmutableMap.of();
    Map<String, String> properties = ImmutableMap.of();
    Assert.assertTrue("Default should be used if no override exists",
        PropertyUtil.resolveBooleanProperty(options, "option-key", properties, "property-key", true));
  }
}
