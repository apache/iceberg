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
package org.apache.iceberg.view;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** Utility methods for operating on common views */
public class ViewUtil {
  /**
   * Method picks and returns the 'summary' properties from the map of table properties. Summary
   * properties are recorded in the 'summary' portion of 'Version' in metadata json file.
   *
   * @param operation The view operation that results in alteration of the view
   * @param properties Map of all table properties
   * @param prevProperties Properties previously set
   * @return A map of summary properties to be recorded in the metadata json file. These are all
   *     previously set properties overlaid with the new properties.
   */
  public static Map<String, String> buildSummaryProperties(
      ViewDDLOperation operation,
      Map<String, String> properties,
      Map<String, String> prevProperties) {
    Map<String, String> props = Maps.newHashMap();
    for (ViewConstants.SummaryConstants key : ViewConstants.SummaryConstants.values()) {
      String val = properties.get(key.name());
      if (val != null) {
        props.put(String.valueOf(key), val);
      } else if (prevProperties != null) {
        val = prevProperties.get(key.name());
        if (val != null) {
          props.put(String.valueOf(key), val);
        }
      }
    }
    props.put(ViewConstants.OPERATION, operation.operationName());
    return props;
  }

  /**
   * Method picks and returns common view specific properties from the map of table properties.
   * These properties are recorded in the 'properties' section of the view version metadata file.
   * Any properties that were previously set and are not being overridden are persisted.
   *
   * @param properties Map of all table properties
   * @param prevProperties Properties that were previously set
   * @param summaryProperties 'summary' portion of 'Version' in metadata json file.
   * @return A map of properties to be recorded in the metadata json file.
   */
  public static Map<String, String> getViewVersionMetadataProperties(
      Map<String, String> properties,
      Map<String, String> prevProperties,
      Map<String, String> summaryProperties) {
    Map<String, String> props = Maps.newHashMap(prevProperties);
    props.putAll(properties);
    props.keySet().removeAll(summaryProperties.keySet());
    return props;
  }

  /**
   * The method prepares the arguments to perform the commit and then proceeds to commit.
   *
   * @param operation View operation causing the commit
   * @param versionId Current version id.
   * @param parentId Version id of the parent version.
   * @param representations View definition
   * @param properties View properties
   * @param location Location of view metadata
   * @param ops View operations object needed to perform the commit
   * @param prevViewMetadata Previous view version metadata
   */
  public static void doCommit(
      ViewDDLOperation operation,
      long versionId,
      Long parentId,
      List<ViewRepresentation> representations,
      Map<String, String> properties,
      String location,
      ViewOperations ops,
      ViewMetadata prevViewMetadata) {
    Map<String, String> prevSummaryProps;
    Map<String, String> prevViewVersionMetadataProps;

    if (prevViewMetadata != null) {
      prevSummaryProps = prevViewMetadata.currentVersion().summary();
      prevViewVersionMetadataProps = prevViewMetadata.properties();
    } else {
      prevSummaryProps = Maps.newHashMap();
      prevViewVersionMetadataProps = Maps.newHashMap();
    }

    // The input set of view properties need to be classified in three sets of properties:
    // 1) Summary properties: these are recorded with a particular version of the view
    //                        (Defined in ViewConstants.java)
    // 2) View version metadata properties: these are not versioned. These are all the other table
    // properties
    //                                      that do not belong in 1) or 2) above.
    Map<String, String> summary =
        ViewUtil.buildSummaryProperties(operation, properties, prevSummaryProps);

    Map<String, String> viewVersionMetadataProperties =
        ViewUtil.getViewVersionMetadataProperties(
            properties, prevViewVersionMetadataProps, summary);
    // add the owner field to the view version metadata props as it doesn't belong to the other 2
    // categories.
    if (properties.get(ViewConstants.OWNER) != null) {
      viewVersionMetadataProperties.put(ViewConstants.OWNER, properties.get(ViewConstants.OWNER));
    }

    BaseViewVersion version =
        BaseViewVersion.builder()
            .versionId(versionId)
            .parentId(parentId)
            .timestampMillis(System.currentTimeMillis())
            .summary(summary)
            .representations(representations)
            .build();

    ViewMetadata.Builder viewVersionMetadataBuilder =
        ViewMetadata.builder().location(location).properties(viewVersionMetadataProperties);
    if (prevViewMetadata != null) {
      viewVersionMetadataBuilder
          .versions(prevViewMetadata.versions())
          .history(prevViewMetadata.history());
    }
    viewVersionMetadataBuilder.addVersion(version);
    ViewMetadata viewMetadata = viewVersionMetadataBuilder.build();

    ops.commit(prevViewMetadata, viewMetadata, viewVersionMetadataProperties);
  }

  private ViewUtil() {}
}
