#!/bin/bash
#
# Rama SAIL Module Deployment Script
#
# Builds and deploys the RdfStorageModule to a Rama cluster.
#
# Usage:
#   ./scripts/deploy-module.sh                    # Deploy to localhost with defaults
#   ./scripts/deploy-module.sh --update           # Update existing deployment
#   CONDUCTOR_HOST=rama.example.com ./scripts/deploy-module.sh  # Deploy to remote
#
# Environment Variables:
#   CONDUCTOR_HOST - Conductor hostname (default: localhost)
#   CONDUCTOR_PORT - Conductor port (default: 8080)
#   MODULE_TASKS   - Number of tasks (default: 8)
#   MODULE_THREADS - Number of threads per task (default: 4)
#   REPLICATION    - Replication factor (default: 1)
#
set -e

# Configuration with defaults
CONDUCTOR_HOST="${CONDUCTOR_HOST:-localhost}"
CONDUCTOR_PORT="${CONDUCTOR_PORT:-8080}"
MODULE_TASKS="${MODULE_TASKS:-8}"
MODULE_THREADS="${MODULE_THREADS:-4}"
REPLICATION="${REPLICATION:-1}"

# Project paths
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MODULE_JAR="$PROJECT_DIR/target/rama-sail-0.1.0-SNAPSHOT-standalone.jar"
MODULE_CLASS="rama_sail.core.RdfStorageModule"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo " Rama SAIL Module Deployment"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  Conductor:   ${CONDUCTOR_HOST}:${CONDUCTOR_PORT}"
echo "  Tasks:       ${MODULE_TASKS}"
echo "  Threads:     ${MODULE_THREADS}"
echo "  Replication: ${REPLICATION}"
echo ""

# Parse command line arguments
UPDATE_MODE=false
SKIP_BUILD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --update|-u)
            UPDATE_MODE=true
            shift
            ;;
        --skip-build|-s)
            SKIP_BUILD=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --update, -u      Update existing deployment instead of new deploy"
            echo "  --skip-build, -s  Skip the uberjar build step"
            echo "  --help, -h        Show this help message"
            echo ""
            echo "Environment Variables:"
            echo "  CONDUCTOR_HOST    Conductor hostname (default: localhost)"
            echo "  CONDUCTOR_PORT    Conductor port (default: 8080)"
            echo "  MODULE_TASKS      Number of tasks (default: 8)"
            echo "  MODULE_THREADS    Threads per task (default: 4)"
            echo "  REPLICATION       Replication factor (default: 1)"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Step 1: Build the JAR
if [ "$SKIP_BUILD" = false ]; then
    echo "Step 1: Building module JAR..."
    cd "$PROJECT_DIR"
    lein uberjar

    if [ ! -f "$MODULE_JAR" ]; then
        echo -e "${RED}ERROR: JAR file not found at $MODULE_JAR${NC}"
        echo "Build may have failed. Check the output above."
        exit 1
    fi

    echo -e "${GREEN}JAR built successfully: $MODULE_JAR${NC}"
else
    echo "Step 1: Skipping build (--skip-build)"
    if [ ! -f "$MODULE_JAR" ]; then
        echo -e "${RED}ERROR: JAR file not found at $MODULE_JAR${NC}"
        echo "Run without --skip-build to build first."
        exit 1
    fi
fi
echo ""

# Step 2: Check if rama-cli is available
if ! command -v rama-cli &> /dev/null; then
    echo -e "${YELLOW}WARNING: rama-cli not found in PATH${NC}"
    echo ""
    echo "To install rama-cli:"
    echo "  1. Download from Red Planet Labs"
    echo "  2. Add to PATH"
    echo ""
    echo "Manual deployment commands:"
    echo ""
    if [ "$UPDATE_MODE" = true ]; then
        echo "  rama-cli module-update \\"
        echo "    --conductor ${CONDUCTOR_HOST}:${CONDUCTOR_PORT} \\"
        echo "    --jar $MODULE_JAR \\"
        echo "    --module $MODULE_CLASS"
    else
        echo "  rama-cli module-deploy \\"
        echo "    --conductor ${CONDUCTOR_HOST}:${CONDUCTOR_PORT} \\"
        echo "    --jar $MODULE_JAR \\"
        echo "    --module $MODULE_CLASS \\"
        echo "    --tasks $MODULE_TASKS \\"
        echo "    --threads $MODULE_THREADS \\"
        echo "    --replication $REPLICATION"
    fi
    echo ""
    exit 0
fi

# Step 3: Deploy or Update
if [ "$UPDATE_MODE" = true ]; then
    echo "Step 2: Updating existing module..."
    rama-cli module-update \
        --conductor "${CONDUCTOR_HOST}:${CONDUCTOR_PORT}" \
        --jar "$MODULE_JAR" \
        --module "$MODULE_CLASS"
else
    echo "Step 2: Deploying new module..."

    # Check if module already exists
    if rama-cli module-info --conductor "${CONDUCTOR_HOST}:${CONDUCTOR_PORT}" --module "$MODULE_CLASS" 2>/dev/null; then
        echo ""
        echo -e "${YELLOW}Module already exists. Use --update to update it.${NC}"
        read -p "Do you want to update the existing module? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rama-cli module-update \
                --conductor "${CONDUCTOR_HOST}:${CONDUCTOR_PORT}" \
                --jar "$MODULE_JAR" \
                --module "$MODULE_CLASS"
        else
            echo "Deployment cancelled."
            exit 0
        fi
    else
        rama-cli module-deploy \
            --conductor "${CONDUCTOR_HOST}:${CONDUCTOR_PORT}" \
            --jar "$MODULE_JAR" \
            --module "$MODULE_CLASS" \
            --tasks "$MODULE_TASKS" \
            --threads "$MODULE_THREADS" \
            --replication "$REPLICATION"
    fi
fi

echo ""
echo -e "${GREEN}=========================================="
echo " Deployment Complete!"
echo "==========================================${NC}"
echo ""
echo "Next steps:"
echo "  1. Verify deployment in Rama UI: http://${CONDUCTOR_HOST}:${CONDUCTOR_PORT}"
echo "  2. Run benchmarks:"
echo "     lein run -m rama-sail.bench.cluster.cluster-bench :host ${CONDUCTOR_HOST} :load true"
echo ""
