#!/bin/bash
set -e

# Market Data Comparison Runner with Capture Proxy
# This script:
# 1. Starts a Capture Proxy server that connects to the exchange
# 2. Starts Python and Java clients that connect to the proxy
# 3. Compares the outputs from both clients

# Configuration from environment variables
LISTING_ID="${LISTING_ID:-1}"
DURATION="${DURATION:-60}"
OUTPUT_DIR="${OUTPUT_DIR:-/app/output}"
PROXY_PORT="${PROXY_PORT:-8765}"

echo "=========================================="
echo "Market Data Comparison Test (Proxy Mode)"
echo "=========================================="
echo "Listing ID: $LISTING_ID"
echo "Duration: ${DURATION}s"
echo "Output Directory: $OUTPUT_DIR"
echo "Proxy Port: $PROXY_PORT"
echo "=========================================="

# Output files
PYTHON_OUTPUT="$OUTPUT_DIR/python_listing_${LISTING_ID}.bin"
JAVA_OUTPUT="$OUTPUT_DIR/java_listing_${LISTING_ID}.bin"
PROXY_LOG="$OUTPUT_DIR/proxy_listing_${LISTING_ID}.log"

# Clean up old output files
rm -f "$PYTHON_OUTPUT" "$JAVA_OUTPUT" "$PROXY_LOG"

# Step 1: Start the Capture Proxy server
echo ""
echo "Step 1: Starting Capture Proxy server..."
python -m gnomepy_testing.capture_proxy "$LISTING_ID" \
    --port "$PROXY_PORT" \
    --duration "$DURATION" \
    --log-file "$PROXY_LOG" &
PROXY_PID=$!
echo "Proxy server started (PID: $PROXY_PID)"

# Wait for proxy to be ready by checking the port
echo "Waiting for proxy to be ready..."
while ! nc -z localhost "$PROXY_PORT"; do sleep 1; done
echo "Proxy ready. Continuing..."

# Step 2: Start Python client
echo ""
echo "Step 2: Starting Python client..."
python -m gnomepy_testing.client.proxy_client \
    --host localhost \
    --port "$PROXY_PORT" \
    --output "$PYTHON_OUTPUT" \
    --listing "$LISTING_ID" &
PYTHON_PID=$!
echo "Python client started (PID: $PYTHON_PID)"

# Step 3: Start Java client
echo ""
echo "Step 3: Starting Java client..."
# Find the jar file automatically
JAVA_JAR=$(find /app/java/lib -name "gnome-orchestrator-*.jar" | head -n 1)
if [ -z "$JAVA_JAR" ]; then
    echo "ERROR: Could not find gnome-orchestrator jar file in /app/java/lib"
    exit 1
fi
echo "Using Java jar: $JAVA_JAR"
java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -cp "$JAVA_JAR" group.gnometrading.testing.MarketDataWriterOrchestrator \
    -Dhost=localhost \
    -Dport="$PROXY_PORT" \
    -Doutput="$JAVA_OUTPUT" \
    -Dlisting="$LISTING_ID" &
JAVA_PID=$!
echo "Java client started (PID: $JAVA_PID)"

# Wait for all processes to complete
echo ""
echo "Collecting data for ${DURATION} seconds..."

# Wait for Python client
wait $PYTHON_PID
PYTHON_EXIT=$?

# Wait for Java client
wait $JAVA_PID
JAVA_EXIT=$?

# Wait for proxy
wait $PROXY_PID
PROXY_EXIT=$?

echo ""
echo "Data collection complete!"
echo "Python client exit code: $PYTHON_EXIT"
echo "Java client exit code: $JAVA_EXIT"
echo "Proxy server exit code: $PROXY_EXIT"

# Check if processes succeeded
if [ $PYTHON_EXIT -ne 0 ]; then
    echo "ERROR: Python client failed with exit code $PYTHON_EXIT"
    exit 1
fi

if [ $JAVA_EXIT -ne 0 ]; then
    echo "ERROR: Java client failed with exit code $JAVA_EXIT"
    exit 1
fi

if [ $PROXY_EXIT -ne 0 ]; then
    echo "ERROR: Proxy server failed with exit code $PROXY_EXIT"
    exit 1
fi

# Compare outputs
echo ""
echo "=========================================="
echo "Results"
echo "=========================================="

# Check if files exist
if [ ! -f "$PYTHON_OUTPUT" ]; then
    echo "ERROR: Python output file not found: $PYTHON_OUTPUT"
    exit 1
fi

if [ ! -f "$PROXY_LOG" ]; then
    echo "ERROR: Proxy log file not found: $PROXY_LOG"
    exit 1
fi

# Show file sizes
echo "Proxy log:     $(wc -l < "$PROXY_LOG") messages received from exchange"
echo "Python output: $(wc -c < "$PYTHON_OUTPUT") bytes written"
echo "Java output:   $(wc -c < "$JAVA_OUTPUT") bytes written"

# Run comparison
echo ""
echo "Comparing Python and Java outputs (schema-aware)..."
python -m gnomepy_testing.compare_outputs \
    --python "$PYTHON_OUTPUT" \
    --java "$JAVA_OUTPUT" \
    --ignore-fields timestamp_recv

COMPARE_EXIT=$?

if [ $COMPARE_EXIT -ne 0 ]; then
    echo ""
    echo "ERROR: Comparison failed!"
    exit 1
fi

echo ""
echo "=========================================="
echo "Test Complete!"
echo "=========================================="
echo "Output files:"
echo "  Proxy log:     $PROXY_LOG"
echo "  Python output: $PYTHON_OUTPUT"
echo "  Java output:   $JAVA_OUTPUT"
echo ""

