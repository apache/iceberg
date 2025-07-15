#
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
#

BASE_PATH := docker/iceberg-test-fixtures

ICEBERG ?= latest
ENGINE ?= spark-4.0.0
STORAGE ?= minio
CATALOG ?= rest
PUSH ?= false

ENGINE_NAME := $(word 1,$(subst -, ,$(ENGINE)))
ENGINE_VERSION := $(word 2,$(subst -, ,$(ENGINE)))
CONNECTOR_VERSION := v$(shell echo $(ENGINE_VERSION) | cut -d. -f1,2)

DOCKER_REPO := apache/iceberg
PLATFORMS := linux/amd64,linux/arm64
PUSH_FLAG :=
ifeq ($(PUSH),true)
  PUSH_FLAG := --push
endif

# Build the iceberg engine base image
.PHONY: iceberg-engine
iceberg-engine:
	docker buildx build \
	--platform $(PLATFORMS) \
	--build-arg ENGINE_VERSION=$(ENGINE_VERSION) \
	--build-arg CONNECTOR_VERSION=$(CONNECTOR_VERSION) \
	--tag $(DOCKER_REPO):$(ICEBERG)-$(ENGINE) \
	--file ${BASE_PATH}/${ENGINE_NAME}-base/Dockerfile . \
	$(PUSH_FLAG)

# Build the iceberg engine image with storage
.PHONY: iceberg-engine-storage
iceberg-engine-storage: iceberg-engine
	docker buildx build \
		  --platform $(PLATFORMS) \
		  --build-arg BASE_IMAGE=$(DOCKER_REPO):$(ICEBERG)-$(ENGINE) \
		  --tag $(DOCKER_REPO):$(ICEBERG)-$(ENGINE)-$(STORAGE) \
		  --file ${BASE_PATH}/${STORAGE}/Dockerfile . \
		  $(PUSH_FLAG)

# Build the iceberg engine image with storage and catalog
.PHONY: iceberg-engine-storage-catalog
iceberg-engine-storage-catalog: iceberg-engine-storage
	docker buildx build \
		--platform $(PLATFORMS) \
		--build-arg BASE_IMAGE=$(DOCKER_REPO):$(ICEBERG)-$(ENGINE)-$(STORAGE) \
		--tag $(DOCKER_REPO):$(ICEBERG)-$(ENGINE)-$(STORAGE)-$(CATALOG) \
		--file ${BASE_PATH}/${CATALOG}/Dockerfile . \
		$(PUSH_FLAG)

.PHONY: clean
clean:
	docker rmi $(ENGINE_BASE_IMAGE) || true
	docker rmi $(STORAGE_ENGINE_IMAGE) || true
	docker rmi $(FULL_FIXTURE_IMAGE) || true