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
const latestVersion = '0.12.0'

function setTopLeftVersionDisplay(version) {
    document.getElementById("top-left-version").innerHTML = version
}

function getWarningMessageWithRedirect(oldVersion, newVersion) {
    const redirectLink = window.location.hash.replace(oldVersion, newVersion).replace('#', '').replace('.md', '')
    console.log(redirectLink, "redirectLink")
    return `
    <span id="not-latest-version-warning">
      <i class="fa fa-exclamation-triangle" style="color:#E50914"></i>
      You are not currently looking at the latest <a href="${redirectLink}">stable release</a>
    </span>
    `
}

function displayVersionWarning(version) {
    console.log(version)
    if (version && version !== latestVersion) {
        console.log("Showing display warning")
        document.getElementById("not-latest-version-warning").innerHTML = getWarningMessageWithRedirect(version, latestVersion)
        document.getElementById("not-latest-version-warning").style.display = "inline-block"
    } else {
        if (document.getElementById("not-latest-version-warning")) {
            document.getElementById("not-latest-version-warning").style.display = "none"
        }
    }
}

window.onload = () => {
    displayVersionWarning(null)
}

window.addEventListener('hashchange', function () {
    const semver = /(\d+\.)(\d+\.)(\d)/
    const version_context = window.location.hash.match(semver)
    if (version_context !== null) {
        const version = version_context[0]
        setTopLeftVersionDisplay(version)
    } else {
        setTopLeftVersionDisplay(latestVersion)
    }
    displayVersionWarning(version_context[0])
}, false);