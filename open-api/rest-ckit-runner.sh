#!/bin/bash
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

# Apache Iceberg REST Compatibility Kit Test Runner
# Production-ready script for running REST catalog compatibility tests

set -euo pipefail  # Exit on error, undefined vars, pipe failures
IFS=$'\n\t'       # Secure Internal Field Separator

# Script configuration
readonly SCRIPT_NAME="$(basename "$0")"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly LOG_FILE="${SCRIPT_DIR}/rest-ckit-runner.log"

# Cache configuration
readonly ICEBERG_CACHE_DIR="${HOME}/.iceberg"
readonly CACHE_DIR="${ICEBERG_CACHE_DIR}/cache"
readonly CACHE_MAVEN_DIR="${ICEBERG_CACHE_DIR}/maven"

# Default versions - can be overridden via environment variables
readonly JUNIT_VER="${JUNIT_VER:-1.13.4}"
readonly ICEBERG_VERSION="${ICEBERG_VERSION:-1.10.0}"
readonly MAVEN_WRAPPER_VERSION="${MAVEN_WRAPPER_VERSION:-3.3.4}"

# Runtime configuration
VERBOSE=false
CLEANUP_ON_EXIT=true
JAVA_OPTS="${JAVA_OPTS:-}"
WORK_DIR="${SCRIPT_DIR}"
CLEAR_CACHE=false

# Color codes for output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m' # No Color

# Logging functions
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${timestamp} [${level}] ${message}" | tee -a "$LOG_FILE"
}

log_info() {
    log "INFO" "${BLUE}$*${NC}"
}

log_warn() {
    log "WARN" "${YELLOW}$*${NC}"
}

log_error() {
    log "ERROR" "${RED}$*${NC}"
}

log_success() {
    log "SUCCESS" "${GREEN}$*${NC}"
}

log_debug() {
    if [[ "$VERBOSE" == "true" ]]; then
        log "DEBUG" "$*"
    fi
}

# Error handling
error_exit() {
    log_error "$1"
    exit "${2:-1}"
}

# Cache management functions
setup_cache_directories() {
    log_debug "Setting up cache directories..."
    
    for dir in "$ICEBERG_CACHE_DIR" "$CACHE_DIR" "$CACHE_MAVEN_DIR"; do
        if [[ ! -d "$dir" ]]; then
            if ! mkdir -p "$dir"; then
                error_exit "Failed to create cache directory: $dir"
            fi
            log_debug "Created cache directory: $dir"
        fi
    done
}

clear_cache() {
    log_info "Clearing Iceberg cache..."
    
    if [[ -d "$ICEBERG_CACHE_DIR" ]]; then
        local cache_size
        cache_size=$(du -sh "$ICEBERG_CACHE_DIR" 2>/dev/null | cut -f1 || echo "unknown")
        
        if rm -rf "$ICEBERG_CACHE_DIR"; then
            log_success "Cache cleared successfully (freed: $cache_size)"
        else
            error_exit "Failed to clear cache directory: $ICEBERG_CACHE_DIR"
        fi
    else
        log_info "Cache directory does not exist, nothing to clear"
    fi
}

show_cache_status() {
    log_info "Cache status:"
    
    if [[ -d "$ICEBERG_CACHE_DIR" ]]; then
        local cache_size
        cache_size=$(du -sh "$ICEBERG_CACHE_DIR" 2>/dev/null | cut -f1 || echo "unknown")
        log_info "Cache directory: $ICEBERG_CACHE_DIR"
        log_info "Cache size: $cache_size"
        
        # Show cached files
        if [[ -d "$CACHE_DIR" ]] && [[ -n "$(ls -A "$CACHE_DIR" 2>/dev/null)" ]]; then
            log_info "Cached files:"
            
            # Show individual JAR files in cache root
            find "$CACHE_DIR" -maxdepth 1 -name "*.jar" | while read -r jar_file; do
                if [[ -f "$jar_file" ]]; then
                    local jar_name=$(basename "$jar_file")
                    local jar_size=$(ls -lh "$jar_file" | awk '{print $5}')
                    log_info "  $jar_name ($jar_size)"
                fi
            done
            
            # Show version-specific directories
            find "$CACHE_DIR" -maxdepth 1 -type d -name "iceberg-*" | while read -r version_dir; do
                if [[ -d "$version_dir" ]]; then
                    local version_name=$(basename "$version_dir")
                    local jar_count=$(find "$version_dir" -name "*.jar" | wc -l)
                    log_info "  $version_name/ ($jar_count JAR files)"
                fi
            done
        else
            log_info "No cached files found"
        fi
        
        # Show Maven wrapper status
        if [[ -f "$CACHE_MAVEN_DIR/mvnw" ]]; then
            log_info "Maven wrapper: cached"
        else
            log_info "Maven wrapper: not cached"
        fi
    else
        log_info "Cache directory does not exist: $ICEBERG_CACHE_DIR"
    fi
}


trap 'error_exit "Script interrupted by user" 130' INT
trap 'error_exit "Script terminated" 143' TERM

# Validation functions
validate_java() {
    if ! command -v java &> /dev/null; then
        error_exit "Java is not installed or not in PATH"
    fi
    
    local java_version
    java_version=$(java -version 2>&1 | head -n1 | cut -d'"' -f2)
    log_info "Using Java version: $java_version"
}

validate_curl() {
    if ! command -v curl &> /dev/null; then
        error_exit "curl is not installed or not in PATH"
    fi
}

validate_required_args() {
    local has_catalog_uri=false
    
    for arg in "$@"; do
        if [[ "$arg" =~ ^--iceberg\.rest\.catalog\.uri= ]]; then
            has_catalog_uri=true
            break
        fi
    done
    
    if [[ "$has_catalog_uri" == "false" ]]; then
        error_exit "Required argument --iceberg.rest.catalog.uri is missing"
    fi
}

show_help() {
    cat <<EOF
${SCRIPT_NAME} - Apache Iceberg REST Compatibility Kit Test Runner

USAGE:
    $0 [OPTIONS] --iceberg.rest.catalog.uri=URI

DESCRIPTION:
    Runs the Apache Iceberg REST Compatibility Kit tests against a REST catalog.
    This script downloads necessary dependencies and executes compatibility tests.

REQUIRED OPTIONS:
    --iceberg.rest.catalog.uri=URI          REST catalog base URI

OPTIONAL TEST OPTIONS:
    --iceberg.rest.catalog.warehouse=PATH   Warehouse location (s3://, file://, etc.)
    --iceberg.rest.auth.token=TOKEN         Bearer auth token
    --iceberg.rest.auth.basic=USER:PASS     Basic auth credentials (user:password)
    --iceberg.rest.oauth2.token=TOKEN       OAuth2 token
    --iceberg.rest.client.timeout=MS        Client request timeout in milliseconds
    --iceberg.rest.trust-all                Trust all SSL certificates (TESTING ONLY)
    --iceberg.rest.debug=true               Enable debug logging

SCRIPT OPTIONS:
    --verbose                               Enable verbose output
    --no-cleanup                            Skip cleanup on exit
    --work-dir=PATH                         Set working directory (default: script directory)
    --java-opts=OPTS                        Additional JVM options
    --clear-cache                           Clear all cached files before running
    --cache-status                          Show cache status and exit
    --help                                  Show this help message

ENVIRONMENT VARIABLES:
    JUNIT_VER                               JUnit Platform version (default: ${JUNIT_VER})
    ICEBERG_VERSION                         Iceberg version (default: ${ICEBERG_VERSION})
    MAVEN_WRAPPER_VERSION                   Maven wrapper version (default: ${MAVEN_WRAPPER_VERSION})
    JAVA_OPTS                               Additional JVM options

EXAMPLES:
    # Basic usage
    $0 --iceberg.rest.catalog.uri=http://localhost:8181

    # With authentication
    $0 --iceberg.rest.catalog.uri=https://catalog.example.com \\
       --iceberg.rest.auth.token=your-token
       --iceberg.rest.catalog.warehouse=gcs://my-bucket/warehouse

    # With verbose output and custom warehouse
    $0 --verbose \\
       --iceberg.rest.catalog.uri=http://localhost:8181 \\
       --iceberg.rest.catalog.warehouse=gcs://my-bucket/warehouse

EXIT CODES:
    0    Success
    1    General error
    2    Invalid arguments
    130  Interrupted by user (Ctrl+C)
    143  Terminated

CACHE MANAGEMENT:
    # Show cache status
    $0 --cache-status

    # Clear cache before running tests
    $0 --clear-cache --iceberg.rest.catalog.uri=http://localhost:8181

CACHE LOCATION:
    Cache directory: $ICEBERG_CACHE_DIR
    - JARs & Dependencies: $CACHE_DIR
    - Maven wrapper: $CACHE_MAVEN_DIR

LOGS:
    Script logs are written to: $LOG_FILE

EOF
}

# Secure file download function
secure_download() {
    local url="$1"
    local output="$2"
    local max_retries=3
    local retry_count=0
    
    log_info "Downloading: $url"
    
    while [[ $retry_count -lt $max_retries ]]; do
        if curl -fsSL --connect-timeout 30 --max-time 300 -o "$output" "$url"; then
            log_success "Downloaded: $output"
            return 0
        else
            ((retry_count++))
            log_warn "Download failed (attempt $retry_count/$max_retries)"
            if [[ $retry_count -lt $max_retries ]]; then
                sleep $((retry_count * 2))
            fi
        fi
    done
    
    error_exit "Failed to download $url after $max_retries attempts"
}

# Verify file integrity (basic check)
verify_file() {
    local file="$1"
    local min_size="${2:-1024}"  # Minimum expected file size in bytes
    
    if [[ ! -f "$file" ]]; then
        error_exit "File does not exist: $file"
    fi
    
    local file_size
    file_size=$(stat -c%s "$file" 2>/dev/null || stat -f%z "$file" 2>/dev/null || echo "0")
    
    if [[ "$file_size" -lt "$min_size" ]]; then
        error_exit "File appears corrupted (size: $file_size bytes): $file"
    fi
    
    log_debug "File verified: $file (size: $file_size bytes)"
}

# Parse arguments and map to system properties
parse_arguments() {
    JAVA_PROPS=()
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --help)
                show_help
                exit 0
                ;;
            --verbose)
                VERBOSE=true
                log_info "Verbose mode enabled"
                shift
                ;;
            --no-cleanup)
                CLEANUP_ON_EXIT=false
                log_info "Cleanup disabled"
                shift
                ;;
            --work-dir=*)
                WORK_DIR="${1#*=}"
                if [[ ! -d "$WORK_DIR" ]]; then
                    error_exit "Work directory does not exist: $WORK_DIR" 2
                fi
                log_info "Using work directory: $WORK_DIR"
                shift
                ;;
            --java-opts=*)
                JAVA_OPTS="${1#*=}"
                log_info "Using Java options: $JAVA_OPTS"
                shift
                ;;
            --clear-cache)
                CLEAR_CACHE=true
                log_info "Cache will be cleared before running"
                shift
                ;;
            --cache-status)
                setup_cache_directories
                show_cache_status
                exit 0
                ;;
            --iceberg.rest.*)
                # Validate and sanitize Iceberg REST options
                if [[ "$1" =~ ^--iceberg\.rest\.[a-zA-Z0-9._-]+= ]]; then
                    key=$(echo "$1" | cut -d= -f1 | sed 's/^--//')
                    val=$(echo "$1" | cut -d= -f2-)
                    
                    # Basic input validation for security
                    if [[ ${#val} -gt 1000 ]]; then
                        error_exit "Value too long for $key (max 1000 characters)" 2
                    fi
                    
                    # Warn about insecure options
                    if [[ "$key" == "iceberg.rest.trust-all" ]]; then
                        log_warn "WARNING: trust-all option is enabled - this is insecure and should only be used for testing!"
                    fi
                    
                    JAVA_PROPS+=("-D${key}=${val}")
                    log_debug "Added property: -D${key}=***"
                else
                    error_exit "Invalid Iceberg REST option format: $1" 2
                fi
                shift
                ;;
            --*)
                error_exit "Unknown option: $1. Use --help for usage information." 2
                ;;
            *)
                error_exit "Unexpected argument: $1. Use --help for usage information." 2
                ;;
        esac
    done
}

# Setup dependencies with caching
setup_dependencies() {
    log_info "Setting up dependencies with caching..."
    
    # Setup cache directories
    setup_cache_directories
    
    cd "$WORK_DIR" || error_exit "Cannot change to work directory: $WORK_DIR"
    
    # Setup JUnit JAR with caching
    setup_junit_jar
    
    # Setup Maven wrapper with caching
    setup_maven_wrapper
    
    # Setup Iceberg dependencies with caching
    setup_iceberg_dependencies
    
    log_success "All dependencies are ready"
}

# Setup JUnit JAR with caching
setup_junit_jar() {
    local junit_jar="junit-platform-console-standalone-${JUNIT_VER}.jar"
    local cached_junit_path="${CACHE_DIR}/${junit_jar}"
    local work_junit_path="${WORK_DIR}/${junit_jar}"
    
    # Check if JAR exists in cache
    if [[ -f "$cached_junit_path" ]]; then
        log_info "Using cached JUnit JAR: $cached_junit_path"
        verify_file "$cached_junit_path" 1000000
        
        # Create symlink in work directory if it doesn't exist
        if [[ ! -f "$work_junit_path" ]]; then
            ln -sf "$cached_junit_path" "$work_junit_path"
            log_debug "Created symlink: $work_junit_path -> $cached_junit_path"
        fi
    else
        log_info "Downloading JUnit Platform Console..."
        local junit_url="https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/${JUNIT_VER}/${junit_jar}"
        
        # Download to cache first
        secure_download "$junit_url" "$cached_junit_path"
        verify_file "$cached_junit_path" 1000000
        
        # Create symlink in work directory
        ln -sf "$cached_junit_path" "$work_junit_path"
        log_success "JUnit JAR cached and linked successfully"
    fi
}

# Setup Maven wrapper with caching
setup_maven_wrapper() {
    local cached_maven_dir="${CACHE_MAVEN_DIR}"
    local cached_mvnw="${cached_maven_dir}/mvnw"
    local cached_wrapper_jar="${cached_maven_dir}/.mvn/wrapper/maven-wrapper.jar"
    local work_mvnw="${WORK_DIR}/mvnw"
    local work_wrapper_dir="${WORK_DIR}/.mvn/wrapper"
    local work_wrapper_jar="${work_wrapper_dir}/maven-wrapper.jar"
    
    # Check if Maven wrapper exists in cache
    if [[ -f "$cached_mvnw" ]] && [[ -f "$cached_wrapper_jar" ]]; then
        log_info "Using cached Maven wrapper"
        
        # Create symlinks in work directory
        if [[ ! -f "$work_mvnw" ]]; then
            ln -sf "$cached_mvnw" "$work_mvnw"
            log_debug "Created symlink: $work_mvnw -> $cached_mvnw"
        fi
        
        if [[ ! -f "$work_wrapper_jar" ]]; then
            mkdir -p "$work_wrapper_dir"
            ln -sf "$cached_wrapper_jar" "$work_wrapper_jar"
            log_debug "Created symlink: $work_wrapper_jar -> $cached_wrapper_jar"
        fi
    else
        log_info "Downloading Maven wrapper to cache..."
        
        # Download mvnw script
        secure_download "https://raw.githubusercontent.com/apache/maven-wrapper/master/mvnw" "$cached_mvnw"
        chmod +x "$cached_mvnw"
        verify_file "$cached_mvnw" 100
        
        # Download wrapper JAR
        mkdir -p "${cached_maven_dir}/.mvn/wrapper"
        local wrapper_jar_url="https://repo1.maven.org/maven2/org/apache/maven/wrapper/maven-wrapper/${MAVEN_WRAPPER_VERSION}/maven-wrapper-${MAVEN_WRAPPER_VERSION}.jar"
        secure_download "$wrapper_jar_url" "$cached_wrapper_jar"
        verify_file "$cached_wrapper_jar" 10000
        
        # Create symlinks in work directory
        ln -sf "$cached_mvnw" "$work_mvnw"
        mkdir -p "$work_wrapper_dir"
        ln -sf "$cached_wrapper_jar" "$work_wrapper_jar"
        
        log_success "Maven wrapper cached and linked successfully"
    fi
}

# Setup Iceberg dependencies with caching
setup_iceberg_dependencies() {
    local cached_deps_dir="${CACHE_DIR}/iceberg-${ICEBERG_VERSION}"
    local work_lib_dir="${WORK_DIR}/lib"
    
    # Check if dependencies exist in cache
    if [[ -d "$cached_deps_dir" ]] && [[ -n "$(ls -A "$cached_deps_dir" 2>/dev/null)" ]]; then
        log_info "Using cached Iceberg dependencies for version $ICEBERG_VERSION"
        
        # Create symlink to cached dependencies
        if [[ -L "$work_lib_dir" ]]; then
            rm "$work_lib_dir"  # Remove existing symlink
        elif [[ -d "$work_lib_dir" ]]; then
            rm -rf "$work_lib_dir"  # Remove existing directory
        fi
        
        ln -sf "$cached_deps_dir" "$work_lib_dir"
        log_debug "Created symlink: $work_lib_dir -> $cached_deps_dir"
    else
        log_info "Downloading Iceberg dependencies to cache..."
        
        # Create temporary directory for download
        local temp_lib_dir="${WORK_DIR}/lib_temp"
        mkdir -p "$temp_lib_dir"
        
        # Download dependencies using Maven wrapper
        if ! ./mvnw dependency:copy-dependencies \
            -Dartifact="org.apache.iceberg:iceberg-open-api:${ICEBERG_VERSION}:tests" \
            -DoutputDirectory="$temp_lib_dir" \
            -q; then
            rm -rf "$temp_lib_dir"
            error_exit "Failed to download Iceberg dependencies"
        fi
        
        # Verify dependencies were downloaded
        if [[ ! -d "$temp_lib_dir" ]] || [[ -z "$(ls -A "$temp_lib_dir" 2>/dev/null)" ]]; then
            rm -rf "$temp_lib_dir"
            error_exit "Dependencies were not downloaded correctly"
        fi
        
        # Move to cache and create symlink
        mkdir -p "$(dirname "$cached_deps_dir")"
        mv "$temp_lib_dir" "$cached_deps_dir"
        ln -sf "$cached_deps_dir" "$work_lib_dir"
        
        log_success "Iceberg dependencies cached and linked successfully"
    fi
}

# Run the compatibility tests
run_tests() {
    log_info "Starting REST Compatibility Kit tests..."
    
    local junit_jar="junit-platform-console-standalone-${JUNIT_VER}.jar"
    local test_class="org.apache.iceberg.rest.RESTCompatibilityKitCatalogTests"
    
    # Build Java command
    local java_cmd=(
        "java"
    )
    
    # Add JVM options if specified
    if [[ -n "$JAVA_OPTS" ]]; then
        # Split JAVA_OPTS safely
        read -ra opts <<< "$JAVA_OPTS"
        java_cmd+=("${opts[@]}")
    fi
    
    # Add system properties
    java_cmd+=("${JAVA_PROPS[@]}")
    
    # Add JAR and test selection
    java_cmd+=(
        "-jar" "$junit_jar"
        "--class-path" "lib/*"
        "--select-class" "$test_class"
    )
    
    log_info "Executing: ${java_cmd[*]}"
    log_debug "Java properties: ${JAVA_PROPS[*]}"
    
    # Run the tests
    if "${java_cmd[@]}"; then
        log_success "REST Compatibility Kit tests completed successfully!"
        return 0
    else
        local exit_code=$?
        log_error "REST Compatibility Kit tests failed with exit code: $exit_code"
        return $exit_code
    fi
}

# Main execution function
main() {
    log_info "Starting Apache Iceberg REST Compatibility Kit Test Runner"
    log_info "Working directory: $WORK_DIR"
    log_info "Iceberg version: $ICEBERG_VERSION"
    log_info "Cache directory: $ICEBERG_CACHE_DIR"
    
    # Parse command line arguments
    parse_arguments "$@"
    
    # Handle cache clearing if requested
    if [[ "$CLEAR_CACHE" == "true" ]]; then
        clear_cache
        setup_cache_directories
    fi
    
    # Validate environment and arguments
    validate_java
    validate_curl
    validate_required_args "$@"
    
    # Setup and run tests
    setup_dependencies
    run_tests
    
    log_success "Script completed successfully!"
}

# Execute main function with all arguments
main "$@"
