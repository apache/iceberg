BASE_PATH := docker/iceberg-test-fixtures

ENGINE ?= spark
STORAGE ?= minio
CATALOG ?= rest

ENGINE_BASE_IMAGE := apache/iceberg-engine-fixture-base
STORAGE_ENGINE_IMAGE := apache/iceberg-engine-storage-fixture-base
FULL_FIXTURE_IMAGE := apache/iceberg-$(ENGINE)-${STORAGE}-$(CATALOG)

# Build the iceberg engine base image
.PHONY: iceberg-engine
iceberg-engine:
	docker build -t $(ENGINE_BASE_IMAGE) -f ${BASE_PATH}/${ENGINE}-base/Dockerfile .

# Build the iceberg engine image with storage
.PHONY: iceberg-engine-storage
iceberg-engine-storage: iceberg-engine
	docker build -t $(STORAGE_ENGINE_IMAGE) -f ${BASE_PATH}/${STORAGE}/Dockerfile .

# Build the iceberg engine image with storage and catalog
.PHONY: iceberg-engine-storage-catalog
iceberg-engine-storage-catalog: iceberg-engine-storage
	docker build -t $(FULL_FIXTURE_IMAGE) -f ${BASE_PATH}/${CATALOG}/Dockerfile .

.PHONY: clean
clean:
	docker rmi $(ENGINE_BASE_IMAGE) || true
	docker rmi $(STORAGE_ENGINE_IMAGE) || true
	docker rmi $(FULL_FIXTURE_IMAGE) || true