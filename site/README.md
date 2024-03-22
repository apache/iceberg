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

# Iceberg site and documentation

This subproject contains the [MkDocs projects](https://www.mkdocs.org/) that define the non-versioned Iceberg site and the versioned Iceberg documentation. The Iceberg site MkDocs project contains a plugin that builds all the static sites for each version of documentation. These subsites are all weaved together to  make the final Apache Iceberg site and docs with a single build.

## Requirements 

* Python >=3.9
* pip


## Usage

The directory structure in this repository aims to mimic the sitemap hierarchy of the website. This helps contributors find the source files needed when updating or adding new documentation. It's helpful to have some basic understanding of the MkDocs framework defaults.

### MkDocs background 

In MkDocs, the [`docs_dir`](https://www.mkdocs.org/user-guide/configuration/#docs_dir) points to the root directory containing the source markdown files for an MkDocs project. By default, this points to directory named `docs` in the same location as the [`mkdocs.yaml` file](https://www.mkdocs.org/user-guide/configuration/#introduction). Use `mkdocs build`is used to build the project. During the build, MkDocs generates the static site in the [`site_dir`](https://www.mkdocs.org/user-guide/configuration/#site_dir) which becomes the root of that project for the generated site. 

### Iceberg docs layout

The static Iceberg website lives under the `/site` directory, while the versioned documentation lives under the `/docs` of the main Iceberg repository. The `/site/docs` directory is named that way to follow the [MkDocs convention](https://www.mkdocs.org/user-guide/configuration/#docs_dir). The `/docs` directory contains the current state of the versioned documentation with local revisions. Notice that the root `/site` and `/docs` just happened to share the same naming convention as MkDocs but does not correlate to the mkdocs

The static Iceberg site pages are Markdown files that live at `/site/docs/*.md`. The versioned documentation are Markdown files that live at `/docs/docs/*.md` files. You may ask where the older versions of the docs and javadocs are, which is covered later in the build section.

```
.
├── docs (versioned)
│   ├── docs
│   │   ├── assets
│   │   ├── api.md
│   │   ├── ...
│   │   └── table-migration.md
│   └── mkdocs.yml
└── site (non-versioned)
    ├── docs
    │   ├── about.md
    │   ├── ...
    │   └── view-spec.md
    ├── ...
    ├── Makefile
    ├── mkdocs.yml
    └── requirements.txt
```
### Building the versioned docs

The Iceberg versioned docs are committed in two [orphan](https://git-scm.com/docs/gitglossary#Documentation/gitglossary.txt-aiddeforphanaorphan) branches and mounted using [git worktree](https://git-scm.com/docs/git-worktree) at build time: 

 1. [`docs`](https://github.com/apache/iceberg/tree/docs) - contains the state of the documenation source files (`/docs`) during release. These versions are mounted at the `/site/docs/docs/<version>` directory at build time. 
 1. [`javadoc`](https://github.com/apache/iceberg/tree/javadoc) - contains prior statically generated versions of the javadocs mounted at `/site/docs/javadoc/<version>` directory at  build time.

The `latest` version, is a soft link to the most recent [semver version](https://semver.org/) in the `docs` branch. The `nightly` version, is a soft link to the current local state of the `/docs` markdown files.

The docs are built, run, and released using [make](https://www.gnu.org/software/make/manual/make.html). The [Makefile](Makefile) and the [common shell script](dev/common.sh) support the following command:

``` site > make help```
> [build](dev/build.sh): Clean and build the site locally.
> [clean](dev/clean.sh): Clean the local site.
> [deploy](dev/deploy.sh): Clean, build, and deploy the Iceberg docs site.
> help: Show help for each of the Makefile recipes.
> [release](dev/release.sh): Release the current `/docs` as `ICEBERG_VERSION` (`make release ICEBERG_VERSION=<MAJOR.MINOR.PATCH>`).
> [serve](dev/serve.sh): Clean, build, and run the site locally.

To scaffold the versioned docs and build the project, run the `build` recipe. 

```
make build
```

This step will generate the staged source code which blends into the original source code above:

```
./site/
└── docs
    ├── docs
    │   ├── nightly (symlink to /docs/)
    │   ├── latest (symlink to /site/docs/1.4.0/)
    │   ├── 1.4.0 
    │   ├── 1.3.1
    │   └── ...
    ├── javadoc
    │   ├── nightly (currently points to latest)
    │   ├── latest
    │   ├── 1.4.0
    │   ├── 1.3.1
    │   └── ...
    └─.asf.yaml
```

To run this, run the `serve` recipe, which runs the `build` recipe and calls `mkdocs serve`. This will run locally at <http://localhost:8000>.
```
make serve
```

To clear all build files, run `clean`.
```
make clean
```

#### Offline mode

One of the great advantages to the MkDocs material plugin is the [offline feature](https://squidfunk.github.io/mkdocs-material/plugins/offline). You can view the Iceberg docs without the need of a server. To enable OFFLINE builds, add theOFFLINE environment variable to either `build` or `serve` recipes.

```
make build OFFLINE=true
```

> [!WARNING]  
> Building with offline mode disables the [use_directory_urls](https://www.mkdocs.org/user-guide/configuration/#use_directory_urls) setting, ensuring that users can open your documentation directly from the local file system. Do not enable this for releases or deployments. 

## Release process

Deploying the docs is a two step process:

> [!WARNING]  
> The `make release` directive is currently unavailable as we wanted to discuss the best way forward on how or if we should automate the release. It involves taking an existing snapshot of the versioned documentation, and potentially automerging the [`docs` branch](https://github.com/apache/iceberg/tree/docs) and the [`javadoc` branch](https://github.com/apache/iceberg/tree/javadoc) which are independent from the `main` branch. Once this is complete, we can create a pull request with an offline build of the documentation to verify everything renders correctly, and then have the release manager merge that PR to finalize the docs release. So the real process would be manually invoking a docs release action, then merging a pull request.

 1. Release a new version by copying the current `/docs` directory to a new version directory in the `docs` branch and a new javadoc build in the `javadoc` branch.
    ```
    make release ICEBERG_VERSION=${ICEBERG_VERSION}
    ```
 1. Build and push the generated site to `asf-site`.
    ```
    make deploy 
    ```

## Validate Links

### How links work in this project

As mentioned in the MkDocs section, when you build MkDocs `mkdocs build`, MkDocs generates the static site in the [`site_dir`](https://www.mkdocs.org/user-guide/configuration/#site_dir) becomes the root of that project and [all links are relative to it](https://www.mkdocs.org/user-guide/writing-your-docs/#internal-links). Note: The default static docs folder name is `site`, so don't get that folder confused with the top-level `/site/` directory.

```
./site/
├── docs
│   ├── docs
│   │   ├── nightly
│   │   │   ├── docs
│   │   │   └── mkdocs.yml
│   │   ├── latest
│   │   │   ├── docs
│   │   │   └── mkdocs.yml
│   │   └── 1.4.0
│   │       ├── docs
│   │       └── mkdocs.yml
│   └─ javadoc
│      ├── nightly 
│      ├── latest
│      └── 1.4.0
└── mkdocs.yml
```

Since there are multiple MkDocs projects that build independently, links between them will initially cause a warning when building. This occurs when `mkdocs-monorepo-plugin` compiles, it must first build the versioned documentation sites before aggregating the top-level site with the generated. Due to the delayed aggregation of subdocs of `mkdocs-monorepo-plugin` there may be warnings that display for the versioned docs that compile without being able to reference documentation it expects outside of the immediate poject due to being off by one or more directories. In other words, if the relative linking required doesn't mirror the directory layout on disk, these errors will occur. The only place this occurs now is with the nav link to javadoc. For more information, refer to: <https://github.com/backstage/mkdocs-monorepo-plugin#usage>

To ensure the links work, you may use linkchecker to traverse the links on the livesite when you're running locally. This may eventually be used as part of the build unless a more suitable static solution is found.

The main issue with using static analysis tools like [mkdocs-linkcheck](https://pypi.org/project/mkdocs-linkcheck) is that they verify links within a single project and do not yet have the ability to analyse a stitched monorepo that we are building with this site.

A step that hasn't been tested yet is considering to use the [offline plugin](https://squidfunk.github.io/mkdocs-material/setup/building-for-offline-usage/) to build a local offline version and test that the internal offline generated site links all work with mkdocs-linkcheck. This would be much faster and less error prone for internal doc links than depending on a running live site. linkchecker will still be a useful tool to run daily on the site to automate any live linking issues. 

```
pip install linkchecker

./linkchecker http://localhost:8000 -r1 -Fcsv/link_warnings.csv

cat ./link_warnings.csv
```

## Things to consider

 - Do not use static links from within the documentation to the public Iceberg site (i.e. [branching](https://iceberg.apache.org/docs/latest/branching)). If you are running in a local environment and made changes to the page you're linking to, your changes mysteriously won't take effect and you'll be scratching your head unless you happen to notice the url bar change.
 - Only use relative links. If you want to reference the root (the directory where the main mkdocs.yml is located `site` in our case) use "spec.md" vs "/spec.md". Also, static sites should only reference the `docs/*` (see next point), but docs can reference the static content normally (e.g. `branching.md` page which is a versioned page linking to `spec.md` which is a static page).
 - Avoid statically linking a specific version of the documentation ('nightly', 'latest', '1.4.0', etc...) unless it is absolutely relevant to the context being provided. This should almost never be the case unless referencing legacy functionality.
 - When internally linking markdown files to other markdown files, [always use the `.md` suffix](https://github.com/mkdocs/mkdocs/issues/2456#issuecomment-881877986). That will indicate to mkdocs exactly how to treat that link depending on the mode the link is compiled with, e.g. if it becomes a <filename>/index.html or <filename>.html. Using the `.md` extension will work with either mode. 
