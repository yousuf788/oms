#!/usr/bin/env bash
# start-media-driver.sh
# Downloads the Aeron Media Driver JAR (if not present) and starts it.
# Must be running on EVERY machine before starting any OMS service.
#
# Usage:
#   ./scripts/start-media-driver.sh          # start in background
#   ./scripts/start-media-driver.sh --fg     # start in foreground (for debug)

set -e

AERON_VERSION="1.44.1"
JAR_NAME="aeron-all-${AERON_VERSION}.jar"
JAR_URL="https://repo1.maven.org/maven2/io/aeron/aeron-all/${AERON_VERSION}/${JAR_NAME}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_PATH="$SCRIPT_DIR/$JAR_NAME"

# Download JAR if not present
if [ ! -f "$JAR_PATH" ]; then
    echo "[aeron] Downloading Aeron Media Driver v${AERON_VERSION}..."
    wget -q --show-progress -O "$JAR_PATH" "$JAR_URL"
    echo "[aeron] Downloaded to $JAR_PATH"
fi

# Shared memory dir for Aeron
export AERON_DIR="${AERON_DIR:-/dev/shm/aeron-$(id -u)}"
mkdir -p "$AERON_DIR"

JAVA_OPTS="-XX:+UseG1GC -Xms64m -Xmx128m"
DRIVER_CLASS="io.aeron.driver.MediaDriver"

if [ "${1}" = "--fg" ]; then
    echo "[aeron] Starting Aeron Media Driver in FOREGROUND (AERON_DIR=$AERON_DIR) ..."
    exec java $JAVA_OPTS \
        -Daeron.dir="$AERON_DIR" \
        -Daeron.term.buffer.sparse.file=false \
        -cp "$JAR_PATH" "$DRIVER_CLASS"
else
    echo "[aeron] Starting Aeron Media Driver in BACKGROUND (AERON_DIR=$AERON_DIR) ..."
    nohup java $JAVA_OPTS \
        -Daeron.dir="$AERON_DIR" \
        -Daeron.term.buffer.sparse.file=false \
        -cp "$JAR_PATH" "$DRIVER_CLASS" \
        > "$SCRIPT_DIR/media-driver.log" 2>&1 &
    MD_PID=$!
    echo "[aeron] Media Driver started with PID $MD_PID"
    echo "$MD_PID" > "$SCRIPT_DIR/media-driver.pid"

    # Wait up to 5s for driver to be ready
    for i in $(seq 1 10); do
        if [ -S "${AERON_DIR}/aeron-driver-ready" ] 2>/dev/null || \
           ls ${AERON_DIR}/cnc.dat 2>/dev/null; then
            echo "[aeron] Media Driver is ready."
            break
        fi
        sleep 0.5
    done
    echo "[aeron] To stop:  kill \$(cat $SCRIPT_DIR/media-driver.pid)"
fi
