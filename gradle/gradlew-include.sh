# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Downloads the gradle-wrapper.jar if necessary and verifies its integrity.
# Included from /.gradlew

# Extract the Gradle version from gradle-wrapper.properties.
GRADLE_DIST_VERSION="$(grep distributionUrl= "$APP_HOME/gradle/wrapper/gradle-wrapper.properties" | sed 's/^.*gradle-\([0-9.]*\)-[a-z]*.zip$/\1/')"
GRADLE_WRAPPER_SHA256="$APP_HOME/gradle/wrapper/gradle-wrapper-${GRADLE_DIST_VERSION}.jar.sha256"
GRADLE_WRAPPER_JAR="$APP_HOME/gradle/wrapper/gradle-wrapper.jar"
if [ -x "$(command -v sha256sum)" ] ; then
  SHASUM="sha256sum"
else
  if [ -x "$(command -v shasum)" ] ; then
    SHASUM="shasum -a 256"
  else
    echo "Neither sha256sum nor shasum are available, install either." > /dev/stderr
    exit 1
  fi
fi
if [ ! -e "${GRADLE_WRAPPER_SHA256}" ]; then
  # Delete the wrapper jar, if the checksum file does not exist.
  rm -f "${GRADLE_WRAPPER_JAR}"
fi
if [ -e "${GRADLE_WRAPPER_JAR}" ]; then
  # Verify the wrapper jar, if it exists, delete wrapper jar and checksum file, if the checksums
  # do not match.
  JAR_CHECKSUM="$(${SHASUM} "${GRADLE_WRAPPER_JAR}" | cut -d\  -f1)"
  EXPECTED="$(cat "${GRADLE_WRAPPER_SHA256}")"
  if [ "${JAR_CHECKSUM}" != "${EXPECTED}" ]; then
    rm -f "${GRADLE_WRAPPER_JAR}" "${GRADLE_WRAPPER_SHA256}"
  fi
fi
if [ ! -e "${GRADLE_WRAPPER_SHA256}" ]; then
  curl --location --output "${GRADLE_WRAPPER_SHA256}" https://services.gradle.org/distributions/gradle-${GRADLE_DIST_VERSION}-wrapper.jar.sha256 || exit 1
fi
if [ ! -e "${GRADLE_WRAPPER_JAR}" ]; then
  # The Gradle version extracted from the `distributionUrl` property does not contain ".0" patch
  # versions. Need to append a ".0" in that case to download the wrapper jar.
  GRADLE_VERSION="$(echo "$GRADLE_DIST_VERSION" | sed 's/^\([0-9]*[.][0-9]*\)$/\1.0/')"
  curl --location --output "${GRADLE_WRAPPER_JAR}" https://raw.githubusercontent.com/gradle/gradle/v${GRADLE_VERSION}/gradle/wrapper/gradle-wrapper.jar || exit 1
  JAR_CHECKSUM="$(${SHASUM} "${GRADLE_WRAPPER_JAR}" | cut -d\  -f1)"
  EXPECTED="$(cat "${GRADLE_WRAPPER_SHA256}")"
  if [ "${JAR_CHECKSUM}" != "${EXPECTED}" ]; then
    # If the (just downloaded) checksum and the downloaded wrapper jar do not match, something
    # really bad is going on.
    echo "Expected sha256 of the downloaded gradle-wrapper.jar does not match the downloaded sha256!" > /dev/stderr
    exit 1
  fi
fi
