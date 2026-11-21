#!/usr/bin/env bash
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
# Checks that everything the GPU scan and compaction need is in place, before a run:
# the JDK, the TornadoVM SDK and its cuDF shim (with the PRs this module needs), libcudf, the GPU and
# its memory, Spark, the bundle, host memory and disk -- and, given a table directory, whether the
# table's shape is one the GPU takes.
#
#   tornadovm/preflight.sh [table directory ...]    e.g. tornadovm/preflight.sh /data/wh/db/sales
#
# Prints one line per check (OK, WARN or FAIL) and exits 1 if any check fails.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
JDK=21                    # the module and TornadoVM's JDK 21 SDK use preview features of exactly this JDK
MIN_COMPUTE=7.0           # libcudf 26.08 ships code for sm_70 and newer
MIN_DRIVER=525            # CUDA 12
SPARK_LINE=4.2            # Spark 4.2, Scala 2.13
DEVICE_MEMORY=${GPU_DEVICE_MEMORY:-20GB}   # spark-submit-gpu's tornado.device.memory
DRIVER_GB=24              # the driver memory the README uses

fails=0
warns=0
ok()   { printf '  OK    %-13s %s\n' "$1" "$2"; }
warn() { printf '  WARN  %-13s %s\n' "$1" "$2"; warns=$((warns + 1)); }
fail() { printf '  FAIL  %-13s %s\n' "$1" "$2"; fails=$((fails + 1)); }
version_ge() { [ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -1)" = "$2" ]; }
gb() { awk -v k="$1" 'BEGIN { printf "%.1f GB", k / 1024 / 1024 }'; }   # from KiB

echo "iceberg-tornadovm preflight"

# --- JDK
JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/}java"
if ! command -v "$JAVA_BIN" >/dev/null 2>&1; then
    fail jdk "no java found (set JAVA_HOME to JDK $JDK)"
else
    jv=$("$JAVA_BIN" -XshowSettings:properties -version 2>&1 | awk -F'= ' '/java.specification.version/ {print $2}')
    if [ "$jv" = "$JDK" ]; then
        ok jdk "JDK $jv${JAVA_HOME:+ at $JAVA_HOME}"
    else
        fail jdk "JDK $jv; need exactly JDK $JDK (preview features are tied to one JDK release)"
    fi
fi

# --- TornadoVM SDK, its CUDA backend, and the cuDF shim with the PRs this module needs
SHIM=""
if [ -z "${TORNADOVM_HOME:-}" ] || [ ! -d "$TORNADOVM_HOME" ]; then
    fail tornadovm "TORNADOVM_HOME is not set (a TornadoVM SDK built with BACKEND=cuda and CUDF_HOME)"
else
    release=$(awk -F= '/^version=/ {print $2}' "$TORNADOVM_HOME/etc/tornado.release" 2>/dev/null)
    backends=$(awk -F= '/^tornado.backends=/ {print $2}' "$TORNADOVM_HOME"/etc/tornado.backend 2>/dev/null)
    floor=$(awk -F= '/^tornado.jdk.floor=/ {print $2}' "$TORNADOVM_HOME/etc/tornado.jdk" 2>/dev/null)
    if [[ "$backends" != *cuda* ]]; then
        fail tornadovm "${release:-the SDK} at $TORNADOVM_HOME has backends '${backends:-none}'; need the CUDA backend"
    elif [ -n "$floor" ] && [ "$floor" != "$JDK" ]; then
        fail tornadovm "${release:-the SDK} is built for JDK $floor; this module needs the JDK $JDK SDK"
    else
        ok tornadovm "TornadoVM ${release:-?}, backends $backends, at $TORNADOVM_HOME"
    fi
    SHIM="$TORNADOVM_HOME/lib/libtornado-cudf.so"
    if [ ! -f "$SHIM" ]; then
        fail cudf-shim "no lib/libtornado-cudf.so: released SDKs do not ship it; build TornadoVM with make BACKEND=cuda and CUDF_HOME set"
        SHIM=""
    else
        symbols=$(nm -D --defined-only "$SHIM" 2>/dev/null | awk '{print $3}')
        text=$(strings "$SHIM")
        jar=$(ls "$TORNADOVM_HOME"/share/java/tornado/tornado-cudf-*.jar 2>/dev/null | head -1)
        missing=""
        grep -qx tornado_cudf_read_parquet_columns <<< "$symbols" || missing="$missing #1200(readParquetColumns)"
        { [ -n "$jar" ] && unzip -p "$jar" '*/CudfType.class' 2>/dev/null | strings | grep -q DECIMAL64; } || missing="$missing #1215(decimals)"
        grep -q TORNADO_CUDF_POOL_RELEASE_MB <<< "$text" || missing="$missing #1219(pool release)"
        grep -q 'an absent column (index -1)' <<< "$text" || missing="$missing #1220(evolved schemas)"
        grep -qx tornado_cudf_write_parquet_columns_with_strings <<< "$symbols" || missing="$missing #1221(string compaction)"
        if [ -n "$missing" ]; then
            fail cudf-shim "the shim lacks TornadoVM PRs:$missing -- build the SDK from a branch that has them"
        else
            ok cudf-shim "libtornado-cudf.so has #1200, #1215, #1219, #1220 and #1221"
        fi
    fi
fi

# --- libcudf and its libraries on LD_LIBRARY_PATH
if [ -n "$SHIM" ]; then
    unresolved=$(ldd "$SHIM" 2>/dev/null | awk '/not found/ {print $1}' | paste -sd' ')
    if [ -n "$unresolved" ]; then
        fail libcudf "not on LD_LIBRARY_PATH: $unresolved (put every libcudf-cu12 wheel library directory there)"
    else
        cudf=$(ldd "$SHIM" | awk '$1 == "libcudf.so" {print $3}')
        v=$(ls -d "$(dirname "$cudf")"/../../libcudf_cu12-*.dist-info 2>/dev/null | sed 's/.*libcudf_cu12-\(.*\)\.dist-info/\1/' | head -1)
        ok libcudf "libcudf ${v:-?} at $(dirname "$cudf")"
    fi
fi

# --- GPU: compute capability, driver, memory for TornadoVM's budget plus cuDF's own
if ! command -v nvidia-smi >/dev/null 2>&1 || ! nvidia-smi -L >/dev/null 2>&1; then
    fail gpu "no NVIDIA GPU or driver found (nvidia-smi failed)"
else
    IFS=, read -r name cc driver total free < <(nvidia-smi --query-gpu=name,compute_cap,driver_version,memory.total,memory.free \
        --format=csv,noheader,nounits | head -1 | sed 's/, */,/g')
    if version_ge "$cc" "$MIN_COMPUTE"; then
        ok gpu "$name, compute capability $cc"
    else
        fail gpu "$name has compute capability $cc; libcudf needs $MIN_COMPUTE (Volta) or newer"
    fi
    if version_ge "$driver" "$MIN_DRIVER"; then
        ok driver "NVIDIA driver $driver"
    else
        fail driver "NVIDIA driver $driver; CUDA 12 needs $MIN_DRIVER or newer"
    fi
    budget_mb=$(awk -v m="$DEVICE_MEMORY" 'BEGIN { n = m + 0; u = toupper(m); print (u ~ /GB$/ ? n * 1024 : u ~ /MB$/ ? n : n / 1048576) }')
    need_mb=$((${budget_mb%.*} + 2048))
    if [ "$free" -lt "$need_mb" ]; then
        fail gpu-memory "$free MiB free; TornadoVM's $DEVICE_MEMORY plus ~2 GB for cuDF need $need_mb MiB (set GPU_DEVICE_MEMORY lower, or free the GPU)"
        nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv,noheader 2>/dev/null | sed 's/^/                      /'
    else
        ok gpu-memory "$free MiB free of $total MiB (TornadoVM $DEVICE_MEMORY + cuDF)"
    fi
fi

# --- Spark
if [ -z "${SPARK_HOME:-}" ] || [ ! -x "$SPARK_HOME/bin/spark-submit" ]; then
    warn spark "SPARK_HOME is not set; needed for Spark (not for the Java API)"
else
    core=$(ls "$SPARK_HOME"/jars/spark-core_*.jar 2>/dev/null | head -1)
    sv=$(basename "$core" .jar | sed 's/spark-core_//')
    if [[ "$sv" == 2.13-$SPARK_LINE.* ]]; then
        ok spark "Spark ${sv#2.13-} (Scala 2.13) at $SPARK_HOME"
    else
        fail spark "Spark '$sv' at $SPARK_HOME; need Spark $SPARK_LINE with Scala 2.13"
    fi
    leftover=$(pgrep -f 'org.apache.spark.deploy.SparkSubmit' | paste -sd' ')
    [ -n "$leftover" ] && warn spark "SparkSubmit JVMs already running (pids $leftover): they hold the GPU and memory"
fi

# --- the bundle, and whether it is older than the sources it is built from
BUNDLE="$HERE/spark/build/spark-gpu/bin/spark-submit-gpu"
if [ ! -x "$BUNDLE" ]; then
    warn bundle "not built: ./gradlew -DsparkVersions=4.2 :iceberg-tornadovm-spark:sparkGpuBundle (needed for Spark)"
else
    newer=$(find "$HERE/src/main" "$HERE/spark/src/main" -newer "$BUNDLE" -type f 2>/dev/null | head -1)
    if [ -n "$newer" ]; then
        warn bundle "older than the sources (e.g. ${newer#$HERE/}); rebuild it with sparkGpuBundle"
    else
        ok bundle "spark/build/spark-gpu, up to date with the sources"
    fi
fi

# --- host memory and disk (object-store files are cached locally; compaction writes new files)
avail_kb=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
total_kb=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
if [ $((avail_kb / 1024 / 1024)) -lt "$DRIVER_GB" ]; then
    warn host-memory "$(gb "$avail_kb") available of $(gb "$total_kb"); the examples use --driver-memory ${DRIVER_GB}g"
else
    ok host-memory "$(gb "$avail_kb") available of $(gb "$total_kb")"
fi
cache_dir="${ICEBERG_TORNADOVM_CACHE_DIR:-${TMPDIR:-/tmp}}"
cache_kb=$(df -Pk "$cache_dir" 2>/dev/null | awk 'NR==2 {print $4}')
if [ "${cache_kb:-0}" -lt $((16 * 1024 * 1024)) ]; then
    warn disk "$(gb "${cache_kb:-0}") free in $cache_dir, the object-store cache (iceberg.tornadovm.cacheDir; cacheBytes defaults to 64 GB)"
else
    ok disk "$(gb "$cache_kb") free in $cache_dir (object-store cache)"
fi

# --- tables: format, column types, sort order, partition spec, location
for table in "$@"; do
    meta=""
    if [ -f "$table/metadata/version-hint.text" ]; then
        meta="$table/metadata/v$(cat "$table/metadata/version-hint.text").metadata.json"
    fi
    [ -f "$meta" ] || meta=$(ls "$table"/metadata/*.metadata.json 2>/dev/null | sort -V | tail -1)
    if [ -z "$meta" ] || [ ! -f "$meta" ]; then
        fail table "$table: no metadata/*.metadata.json (pass a table directory)"
        continue
    fi
    while IFS='|' read -r level message; do
        case "$level" in
            OK) ok table "$message" ;;
            WARN) warn table "$message" ;;
            *) fail table "$message" ;;
        esac
    done < <(python3 - "$meta" "$(basename "$table")" <<'EOF'
import json, sys
m = json.load(open(sys.argv[1])); name = sys.argv[2]
schema = next(s for s in m["schemas"] if s["schema-id"] == m.get("current-schema-id", 0))
read_ok = {"int", "long", "float", "double", "date", "time", "timestamp", "timestamptz", "string"}
def kind(t):
    if isinstance(t, dict):
        return t["type"]
    if t.startswith("decimal("):
        p = int(t[8:].split(",")[0])
        return "decimal" if p <= 18 else "decimal>18"
    return t
fields = [(f["name"], kind(f["type"])) for f in schema["fields"]]
nested = [n for n, t in fields if t in ("struct", "list", "map")]
other = sorted({t for _, t in fields if t not in read_ok | {"decimal"}} - {"struct", "list", "map"})
snap = next((s for s in m.get("snapshots", []) if s["snapshot-id"] == m.get("current-snapshot-id")), None)
summary = snap.get("summary", {}) if snap else {}
files = summary.get("total-data-files", "?")
deletes = int(summary.get("total-position-deletes", 0)) + int(summary.get("total-equality-deletes", 0))
state = f"{files} data files, {deletes:,} deleted rows pending" if snap else "no snapshot yet"
print(f"OK|{name}: format v{m['format-version']}, {len(fields)} columns, {state}")
if nested:
    print(f"WARN|{name}: nested columns {', '.join(nested)} are not read on the GPU; scans projecting them stay on Spark")
if other:
    print(f"WARN|{name}: column types {', '.join(other)} are not read on the GPU; scans and rewrites that need them stay on Spark")
sorted_order = any(o.get("fields") for o in m.get("sort-orders", []) if o["order-id"] == m.get("default-sort-order-id", 0))
specs = m.get("partition-specs", [])
evolved = len(specs) > 1
if sorted_order:
    print(f"WARN|{name}: has a sort order; compaction of sorted tables stays on Spark")
if evolved:
    print(f"WARN|{name}: its partition spec evolved; compaction stays on Spark (scans still run on the GPU)")
loc = m.get("location", "")
scheme = loc.split("://")[0] if "://" in loc else "file"
if scheme not in ("file",):
    print(f"OK|{name}: on {scheme}://, read through the local file cache (iceberg.tornadovm.cacheDir)")
if not (nested or other or sorted_order or evolved):
    print(f"OK|{name}: scans and compaction can run on the GPU")
EOF
)
done

echo
if [ "$fails" -gt 0 ]; then
    echo "preflight: $fails check(s) failed, $warns warning(s)"
    exit 1
fi
[ "$warns" -gt 0 ] && echo "preflight: ready, with $warns warning(s)" || echo "preflight: ready"
