<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

## Apache Iceberg Site

This directory contains the source for the Iceberg site.

* Site structure is maintained in mkdocs.yml
* Pages are maintained in markdown in the `docs/` folder
* Links use bare page names: `[link text](target-page)`

### Installation

The site is built using mkdocs. To install mkdocs and the theme, run:

```
pip install -r requirements.txt
```

### Local Changes

To see changes locally before committing, use mkdocs to run a local server from this directory.

```
mkdocs serve
```

To see changes in Javadoc, run:

```
./gradlew refreshJavadoc
```

### Publishing

After site changes are committed, you can publish the site with this command:

```
./gradlew deploySite
```

This assumes that the Apache remote is named `apache` and will push to the
`asf-site` branch. You can specify the name of a different remote by appending
`-Premote.name=<remote-name>` to the `./gradlew deploySite` command.

### Releasing Docs for a New Version

To add docs for a new version, there are a few steps. To describe this, we'll use a simple alias of
`0.0.1` and `0.0.2` to represent the current latest existing version docs and the newer version of the docs
we want to add, respectively.

With `0.0.1` as the current latest docs, the `site` directory looks something like this.
```
.
├── README.md
├── docs
│   ├── 0.0.1
│   │   └── ...
│   ├── css
│   │   └── ...
│   ├── img
│   │   └── ...
│   ├── js
│   │   └── ...
│   ├── theme_customization
│   │   └── ...
│   ├── index.md
│   └── ...
├── mkdocs.yml
└── requirements.txt
```

The first thing we have to do to add a new version to the docs site, is make a `0.0.2` directory,
and it almost always makes sense to start with the previous version's docs.
```java
cp -r docs/0.0.1 docs/0.0.2
```

Next, make all of the necessary changes to the `0.0.2` docs in the `0.0.2` directory.

In the `mkdocs.yml` file, **copy** the entire `nav:` section, and nest it within a `v0.0.1` block, then
change the top-level links to point to the `0.0.2` directory.
#### mkdocs.yml (before)
```java
nav:
  - Tables:
      - Configuration: 0.0.1/tables/configuration.md
      - ...
  - ...
```
#### mkdocs.yml (after)
```java
nav:
  - Tables:
      - Configuration: 0.0.2/tables/configuration.md
      - ...
  - ...
  - "v0.0.1":
      - Tables:
          - Configuration: 0.0.1/tables/configuration.md
          - ...
      - ...
```

Additionally, to help narrow the search functionality to the latest version only, add the directory
containing the now previous version to the `exclude-search` section.
```yaml
  - exclude-search:
      exclude:
        - 0.0.1/*
```

The last steps require slight changes to the HTML and javascript that powers the version dropdown
and the warning labels that show up when you're viewing an outdated version.

In `docs/theme_customization/topbar.html`, change link for `0.0.1` to point to the new location, and
add an element for the new latest version `0.0.2` that links to the homepage.
#### topbar.html (before)
```html
...
<span class="wm-version-dropdown">
  <a href="/">
    0.0.1
  </a>
</span>
...
```
#### topbar.html (after)
```html
...
<span class="wm-version-dropdown">
  <a href="/">
    0.0.2
  </a>
</span>
<span class="wm-version-dropdown">
  <a href="/#0.0.1/tables/configuration.md">
    0.0.1
  </a>
</span>
...
```

Next, at the top of `docs/js/extra.js`, change `latestVersion` to the new latest version `0.0.2`.
#### extra.js (before)
```js
const latestVersion = '0.0.1'
```
#### extra.js (after)
```js
const latestVersion = '0.0.2'
```

Lastly, edit the `all-versions.md` page to include a link to the now previous version `0.0.1`.
#### all-versions.md
```md
## Documenation for Previous Versions

[0.0.1](/#0.0.1/tables/configuration)  
```
