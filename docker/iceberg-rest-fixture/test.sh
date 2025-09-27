#!/bin/bash

set -euo pipefail  # Exit on error, treat unset variables as error, fail on pipeline errors

# Define variables
IMAGE_NAME="iceberg-rest-fixture:test"
DOCKERFILE_PATH="docker/iceberg-rest-fixture/Dockerfile"
CONTAINER_NAME="iceberg-rest-test"
PORT=8181

# Step 1: Build the required JAR using Gradle
echo "Building the open-api module JAR..."
./gradlew :open-api:build

# Step 2: Build the Docker image
echo "Building Docker image from $DOCKERFILE_PATH..."
docker build -t $IMAGE_NAME -f $DOCKERFILE_PATH .

# Step 3: Run the container in detached mode
echo "Starting container..."
docker run -d --name $CONTAINER_NAME -p $PORT:$PORT $IMAGE_NAME

# Step 4: Wait for the container to start (healthcheck interval is 1s, retries 10, so wait up to 15s)
echo "Waiting for healthcheck..."
sleep 15

# Step 5: Check if the container is running
if [ "$(docker inspect -f '{{.State.Running}}' $CONTAINER_NAME)" != "true" ]; then
  echo "Error: Container failed to start."
  docker logs $CONTAINER_NAME
  exit 1
fi

# Step 6: Perform healthcheck manually (matches the Dockerfile's HEALTHCHECK)
echo "Running healthcheck..."
if ! curl --fail http://localhost:$PORT/v1/config; then
  echo "Error: Healthcheck failed."
  docker logs $CONTAINER_NAME
  exit 1
fi

# Step 7: Optional: Basic functionality check 
CONFIG_RESPONSE=$(curl -s http://localhost:$PORT/v1/config)
if [[ ! $CONFIG_RESPONSE =~ "catalogs" ]]; then  
  echo "Error: Config endpoint response unexpected."
  echo "Response: $CONFIG_RESPONSE"
  exit 1
fi

# Cleanup
echo "Cleaning up..."
docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME
docker rmi $IMAGE_NAME

echo "All tests passed!"
