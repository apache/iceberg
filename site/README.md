## Apache Iceberg Site

This directory contains the source for the Iceberg site.

* Site struture is maintained in mkdocs.yml
* Pages are maintained in markdown in the `docs/` folder
* Links use bare page names: `[link text](target-page)`

### Installation

The site is built using mkdocs. To install mkdocs and the theme, run:

```
pip install mkdocs
pip install mkdocs-cinder
```

### Local Changes

To see changes locally before committing, use mkdocs to run a local server from this directory.

```
mkdocs serve
```

### Publishing

After site changes are committed, you can publish the site with this command:

```
mkdocs gh-deploy
```

This assumes that the Apache remote is named `apache` and will push to the `asf-site` branch. To use a different remote add `-r <remote-name>`.
