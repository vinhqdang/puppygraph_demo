#!/bin/bash

# Complete benchmark runner - manages all databases and runs the complete test
# This script:
# 1. Starts all 3 databases with Docker (PostgreSQL, Neo4j, PuppyGraph)
# 2. Runs the complete Python benchmark
# 3. Stops all Docker containers when done

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Cleanup function to stop databases
cleanup() {
    echo ""
    print_info "Stopping all databases..."
    docker compose down
    print_success "All databases stopped"
}

# Register cleanup on exit
trap cleanup EXIT

echo "========================================"
echo "Complete Graph Database Benchmark"
echo "========================================"
echo ""

# Step 1: Check Docker
print_info "Checking Docker..."
if ! docker ps &> /dev/null; then
    print_error "Docker is not running. Please start Docker first."
    exit 1
fi
print_success "Docker is running"

# Step 2: Start databases
echo ""
print_info "Starting all databases (PostgreSQL, Neo4j, PuppyGraph)..."
docker compose up -d

# Step 3: Wait for databases to be ready
echo ""
print_info "Waiting for databases to be ready..."

# Wait for PostgreSQL
echo -n "PostgreSQL: "
for i in {1..30}; do
    if docker exec puppygraph_postgres pg_isready -U postgres &> /dev/null; then
        print_success "Ready"
        break
    fi
    echo -n "."
    sleep 1
done

# Wait for Neo4j
echo -n "Neo4j: "
for i in {1..60}; do
    if docker exec puppygraph_neo4j wget --no-verbose --tries=1 --spider http://localhost:7474 &> /dev/null 2>&1; then
        print_success "Ready"
        break
    fi
    echo -n "."
    sleep 1
done

# Wait for PuppyGraph
echo -n "PuppyGraph: "
for i in {1..30}; do
    if docker exec puppygraph_puppygraph wget --no-verbose --tries=1 --spider http://localhost:8081 &> /dev/null 2>&1; then
        print_success "Ready"
        break
    fi
    echo -n "."
    sleep 1
done

# Step 4: Show database info
echo ""
echo "========================================"
echo "Databases are ready!"
echo "========================================"
echo "PostgreSQL: localhost:5432 (user: postgres, password: postgres)"
echo "Neo4j Bolt: localhost:7687 (user: neo4j, password: password)"
echo "Neo4j UI:   http://localhost:7474"
echo "PuppyGraph: localhost:8081, localhost:8182"
echo ""

# Step 5: Run benchmark
print_info "Running benchmark..."
echo ""
print_info "Note: Make sure you have activated the conda environment before running this script"
echo ""

# Run the complete benchmark
python run_all.py

echo ""
print_success "Benchmark completed successfully!"
echo ""
print_info "Results saved to results/ directory"
echo "  - benchmark_summary.csv"
echo "  - performance_comparison.png"
echo "  - Detailed results for each database"

echo ""
echo "========================================"
echo "Benchmark Complete!"
echo "========================================"
