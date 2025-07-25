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
package org.apache.iceberg.rest.auth.oauth2.docs;

import com.thoughtworks.qdox.JavaProjectBuilder;
import com.thoughtworks.qdox.model.JavaClass;
import com.thoughtworks.qdox.model.JavaField;
import com.thoughtworks.qdox.model.JavaMethod;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Parses the properties from the source code and generates documentation for them.
 *
 * <p>This generator is mostly intended to parse the `OAuth2Config` class. The parser relies heavily
 * on conventions, such as the use of `PREFIX` fields, or fields starting with `DEFAULT_`, or the
 * presence of nested classes to structure the properties into sections.
 */
@SuppressWarnings("ParameterAssignment")
public class OAuth2DocumentationGenerator {

  private static final String HEADER =
      """
      <!--
       - Licensed to the Apache Software Foundation (ASF) under one or more
       - contributor license agreements.  See the NOTICE file distributed with
       - this work for additional information regarding copyright ownership.
       - The ASF licenses this file to You under the Apache License, Version 2.0
       - (the "License"); you may not use this file except in compliance with
       - the License.  You may obtain a copy of the License at
       -
       -   http://www.apache.org/licenses/LICENSE-2.0
       -
       - Unless required by applicable law or agreed to in writing, software
       - distributed under the License is distributed on an "AS IS" BASIS,
       - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
       - See the License for the specific language governing permissions and
       - limitations under the License.
       -->

      <!--
      This page is automatically generated from the code. Do not edit it manually.
      To update this page, run: `./gradlew :iceberg-core:generateOAuth2Docs`.
      -->

      # REST OAuth2 Configuration

      """;

  private static final Pattern CODE_PATTERN = Pattern.compile("\\{@code\\s(.+?)}");

  private static final Pattern REF_PATTERN =
      Pattern.compile("\\{@(?:link(?:plain)?|value)\\s+([^ }]+)( [^}]+)?}");

  private static final Pattern EXTERNAL_LINK_PATTERN =
      Pattern.compile("<a\\s+href=\"([^\"]+)\"[^>]*>([^<]+)</a>");

  private static final Pattern PRE_PATTERN =
      Pattern.compile("<pre>\\s*(?:\\{@code)?(.*?)(}\\s*)?</pre>", Pattern.DOTALL);

  private static final Map<String, String> KNOWN_REFS;

  static {
    Map<String, String> refs = new LinkedHashMap<>();
    refs.put("GrantType#CLIENT_CREDENTIALS", "client_credentials");
    refs.put("GrantType#REFRESH_TOKEN", "refresh_token");
    refs.put("GrantType#TOKEN_EXCHANGE", "urn:ietf:params:oauth:grant-type:token-exchange");
    refs.put("ClientAuthenticationMethod#NONE", "none");
    refs.put("ClientAuthenticationMethod#CLIENT_SECRET_BASIC", "client_secret_basic");
    refs.put("ClientAuthenticationMethod#CLIENT_SECRET_POST", "client_secret_post");
    KNOWN_REFS = Map.copyOf(refs);
  }

  private static final String ROOT_CLASS_NAME = "org.apache.iceberg.rest.auth.oauth2.OAuth2Config";

  private final Path rootConfigFile;
  private final Path outputFile;

  private JavaProjectBuilder builder;
  private String rootPrefix;
  private Map<String, Section> sections;

  public static void main(String[] args) throws IOException {
    String sourceFile = args[0];
    String outputFile = args[1];
    OAuth2DocumentationGenerator generator =
        new OAuth2DocumentationGenerator(Path.of(sourceFile), Path.of(outputFile));
    generator.run();
  }

  public OAuth2DocumentationGenerator(Path rootConfigFile, Path outputFile) {
    this.rootConfigFile = rootConfigFile;
    this.outputFile = outputFile;
  }

  public void run() throws IOException {
    parse();
    generate();
  }

  private void parse() throws IOException {

    builder = new JavaProjectBuilder();
    builder.addSource(rootConfigFile.toFile());

    File[] files =
        rootConfigFile
            .resolveSibling("config")
            .toFile()
            .listFiles(file -> file.getName().endsWith("Config.java"));
    Preconditions.checkNotNull(files, "Failed to list config files");

    for (File file : files) {
      builder.addSource(file);
    }

    JavaClass topClass = builder.getClassByName(ROOT_CLASS_NAME);
    rootPrefix = topClass.getFieldByName("PREFIX").getInitializationExpression().replace("\"", "");

    sections = new LinkedHashMap<>();

    for (JavaMethod method : topClass.getMethods()) {

      if (method.getName().matches("\\w+Config")) {
        JavaClass sectionConfigClass = (JavaClass) method.getReturnType();
        sections.put(sectionConfigClass.getFullyQualifiedName(), new Section(sectionConfigClass));
      }
    }

    for (Section section : sections.values()) {
      section.parseProperties();
    }
  }

  private void generate() throws IOException {

    try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
      writer.write(HEADER);

      for (Section section : sections.values()) {

        writer.write("## " + section.name + "\n\n");
        if (section.description != null && !section.description.isEmpty()) {
          writer.write(section.description);
        }

        for (Property property : section.properties) {
          writer.write("\n### `" + property.name + "`\n\n");
          writer.write(property.description);
        }
      }
    }
  }

  @SuppressWarnings({"UnnecessaryUnicodeEscape", "AvoidEscapedUnicodeCharacters"})
  private String sanitizeDescription(Section section, String text) {

    Matcher matcher = EXTERNAL_LINK_PATTERN.matcher(text);
    StringBuilder sb = new StringBuilder();
    while (matcher.find()) {
      String url = matcher.group(1);
      String linkText = matcher.group(2);
      matcher.appendReplacement(sb, "[" + linkText + "](" + url + ")");
    }
    matcher.appendTail(sb);
    text = sb.toString();

    matcher = CODE_PATTERN.matcher(text);
    sb = new StringBuilder();
    while (matcher.find()) {
      String codeBlock = matcher.group(1);
      matcher.appendReplacement(sb, "`" + codeBlock + "`");
    }
    matcher.appendTail(sb);
    text = sb.toString();

    matcher = REF_PATTERN.matcher(text);
    sb = new StringBuilder();
    while (matcher.find()) {
      String fieldRef = matcher.group(1);
      String refText = matcher.group(2);
      String resolvedReference = resolveReference(section, fieldRef, refText);
      matcher.appendReplacement(sb, resolvedReference);
    }
    matcher.appendTail(sb);
    text = sb.toString();

    text = cleanupHtmlTags(text);

    // Clean up extra whitespace and normalize line breaks
    text = text.replaceAll("\\r\\n", "\n");
    text = text.replaceAll("\\r", "\n");
    text = text.replaceAll("\n(?![\n\\-])", " ");
    text = text.replaceAll(" {2,}", " ");
    text = text.replaceAll("\n ", "\n");
    text = text.replaceAll("\n{3,}", "\n\n");
    text = text.replaceAll("\u2424", "\n");
    text = text.trim() + "\n";

    return text;
  }

  private String resolveReference(Section section, String ref, String text) {
    String refTarget = KNOWN_REFS.get(ref);
    if (refTarget == null) {
      if (ref.equals("OAuth2Config#PREFIX")) {
        refTarget = rootPrefix;
      } else if (ref.startsWith("#")) {
        // local ref
        String fieldName = ref.substring(1);
        refTarget = section.refs.get(fieldName);
      } else if (section != null) {
        // external ref
        List<String> parts = Splitter.on('#').splitToList(ref);
        String className = section.configClass.getPackageName() + "." + parts.get(0);
        String fieldName = parts.get(1);
        JavaClass classRef = builder.getClassByName(className);
        Section refSection = sections.get(classRef.getFullyQualifiedName());
        refTarget = refSection.refs.get(fieldName);
      }
    }
    if (text == null) {
      return "`" + refTarget + "`";
    }
    text = text.trim();
    return text.isEmpty() || text.equals(refTarget)
        ? "`" + refTarget + "`"
        : text + " (`" + refTarget + "`)";
  }

  @SuppressWarnings({"UnnecessaryUnicodeEscape", "AvoidEscapedUnicodeCharacters"})
  private static String cleanupHtmlTags(String text) {
    // Convert HTML lists
    text = text.replaceAll("<ul> *", "");
    text = text.replaceAll(" *</ul>", "");
    text = text.replaceAll(" *<li> *", "- ");
    // Convert HTML paragraphs
    text = text.replaceAll("<p>\\s*", "\n");

    // Temporarily replace newlines in code blocks with a different character
    // to preserve them during the next steps. Here we use 'SYMBOL FOR NEWLINE' (U+2424).
    Matcher matcher = PRE_PATTERN.matcher(text);
    StringBuilder sb = new StringBuilder();
    while (matcher.find()) {
      String codeBlock = matcher.group(1).trim();
      codeBlock = codeBlock.replaceAll("[\r\n]", "\u2424");
      matcher.appendReplacement(sb, "\n\n```\u2424" + codeBlock + "\u2424```\n\n");
    }
    matcher.appendTail(sb);
    text = sb.toString();

    return text;
  }

  private class Section {

    private final JavaClass configClass;
    private final String prefix;
    private final String name;
    private final String description;
    private final Map<String, String> refs = Maps.newLinkedHashMap();
    private final List<Property> properties = Lists.newArrayList();

    private Section(JavaClass configClass) {
      this.configClass = configClass;
      this.name = sanitizeSectionName(configClass.getSimpleName());
      this.prefix = resolvePrefix();
      parseLocalReferences();
      this.description = sanitizeDescription(this, configClass.getComment());
    }

    private String sanitizeSectionName(String className) {
      return className.replace("Config", "").replaceAll("([A-Z])", " $1").trim() + " Settings";
    }

    private String resolvePrefix() {
      if (configClass.getSimpleName().equals("BasicConfig")) {
        // Basic config: shares the root prefix
        return rootPrefix;
      } else {
        JavaField prefixField = configClass.getFieldByName("PREFIX");
        return prefixField
            .getInitializationExpression()
            .replace("OAuth2Config.PREFIX + ", rootPrefix)
            .replace("\"", "");
      }
    }

    private void parseLocalReferences() {
      for (JavaField field : configClass.getFields()) {
        String refName = field.getName();
        if (refName.equals("PREFIX")) {
          refs.put("PREFIX", prefix);
        } else if (refName.startsWith("DEFAULT_")) {
          String refText = field.getInitializationExpression();
          if (refText.startsWith("Duration.parse")) {
            refText = refText.substring(refText.indexOf("(\"") + 2, refText.indexOf("\")"));
          } else {
            String asRef = refText.replace('.', '#');
            String resolved = KNOWN_REFS.get(asRef);
            refText = resolved != null ? resolved : refText.replace("\"", "");
          }
          refs.put(refName, refText);
        } else {
          String refText =
              field
                  .getInitializationExpression()
                  .replaceAll("PREFIX \\+\\s*", prefix)
                  .replace("\"", "");
          refs.put(refName, refText);
        }
      }
    }

    private void parseProperties() {
      for (JavaMethod method : configClass.getMethods()) {
        if (method.getComment() == null) {
          continue;
        }
        method.getAnnotations().stream()
            .filter(a -> a.getType().getName().equals("ConfigOption"))
            .findFirst()
            .ifPresent(
                ann -> {
                  String configOption = ann.getNamedParameter("value").toString();
                  String propertyName = refs.get(configOption);
                  String propertyDescription = sanitizeDescription(this, method.getComment());
                  if (ann.getNamedParameter("prefixMap") != null) {
                    boolean prefixMap =
                        Boolean.parseBoolean((String) ann.getNamedParameter("prefixMap"));
                    if (prefixMap) {
                      propertyName = propertyName + ".*";
                    }
                  }
                  properties.add(new Property(propertyName, propertyDescription));
                });
      }
    }
  }

  private record Property(String name, String description) {}
}
