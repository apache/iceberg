#!/bin/bash

set -euo pipefail  # Exit on error, treat unset variables as error, fail on pipeline errors

# Define variables
IMAGE_NAME="iceberg-rest-fixture:test"
DOCKERFILE_PATH="docker/iceberg-rest-fixture/Dockerfile"
CONTAINER_NAME="iceberg-rest-test"
PORT=8181
VERBOSE=${VERBOSE:-false}  # Control output verbosity via environment variable
MAX_HEALTH_CHECK_ATTEMPTS=30  # Maximum attempts for health check (30 seconds)
HEALTH_CHECK_INTERVAL=1  # Seconds between health check attempts

# Helper function for verbose output
log_verbose() {
  if [ "$VERBOSE" = "true" ]; then
    echo "$@"
  fi
}

# Function to wait for container health
wait_for_healthy() {
  local container_name=$1
  local max_attempts=$2
  local interval=$3
  
  echo "Waiting for container to become healthy..."
  for i in $(seq 1 $max_attempts); do
    health_status=$(docker inspect --format='{{.State.Health.Status}}' $container_name 2>/dev/null || echo "unknown")
    
    if [ "$health_status" = "healthy" ]; then
      echo "Container is healthy after $i attempts"
      return 0
    elif [ "$health_status" = "unhealthy" ]; then
      echo "Error: Container became unhealthy"
      docker logs $container_name
      return 1
    fi
    
    log_verbose "Attempt $i/$max_attempts: Container status is '$health_status'"
    sleep $interval
  done
  
  echo "Error: Container did not become healthy within $max_attempts seconds"
  docker logs $container_name
  return 1
}

# Cleanup function to ensure resources are cleaned up on exit
cleanup() {
  echo "Cleaning up..."
  docker stop $CONTAINER_NAME 2>/dev/null || true
  docker rm $CONTAINER_NAME 2>/dev/null || true
  docker rmi $IMAGE_NAME 2>/dev/null || true
}

# Register cleanup function to run on script exit
trap cleanup EXIT

# Step 1: Build the required JAR using Gradle
echo "Building the open-api module JAR..."
if [ "$VERBOSE" = "true" ]; then
  ./gradlew :open-api:build
else
  ./gradlew :open-api:build --quiet
fi

# Step 2: Build the Docker image
echo "Building Docker image from $DOCKERFILE_PATH..."
if [ "$VERBOSE" = "true" ]; then
  docker build -t $IMAGE_NAME -f $DOCKERFILE_PATH .
else
  docker build -t $IMAGE_NAME -f $DOCKERFILE_PATH . --quiet
fi

# Step 3: Run the container in detached mode
echo "Starting container..."
docker run -d --name $CONTAINER_NAME -p $PORT:$PORT $IMAGE_NAME

# Step 4: Wait for the container to become healthy using Docker's health check
if ! wait_for_healthy $CONTAINER_NAME $MAX_HEALTH_CHECK_ATTEMPTS $HEALTH_CHECK_INTERVAL; then
  echo "Error: Container health check failed."
  exit 1
fi

# Step 5: Verify the container is still running
if [ "$(docker inspect -f '{{.State.Running}}' $CONTAINER_NAME)" != "true" ]; then
  echo "Error: Container is not running."
  docker logs $CONTAINER_NAME
  exit 1
fi

# Step 6: Basic functionality check on /v1/config endpoint
# Note: This verifies the application is responding correctly, complementing Docker's HEALTHCHECK
echo "Testing /v1/config endpoint..."
CONFIG_RESPONSE=$(curl -s --fail http://localhost:$PORT/v1/config || echo "")
if [ -z "$CONFIG_RESPONSE" ]; then
  echo "Error: Failed to reach config endpoint."
  docker logs $CONTAINER_NAME
  exit 1
fi

if [[ ! $CONFIG_RESPONSE =~ "catalogs" ]]; then  
  echo "Error: Config endpoint response unexpected."
  echo "Response: $CONFIG_RESPONSE"
  exit 1
fi

echo "All tests passed!"
echo "Config response: $CONFIG_RESPONSE"
