#!/bin/bash

# Quick start script - sets up everything and runs the benchmark

set -e

echo "========================================"
echo "Graph Database Benchmark - Quick Start"
echo "========================================"
echo ""
echo "This script will:"
echo "  1. Setup databases (PostgreSQL & Neo4j)"
echo "  2. Setup Python environment"
echo "  3. Run the complete benchmark"
echo ""
read -p "Press Enter to continue or Ctrl+C to cancel..."
echo ""

# Step 1: Setup databases
echo "Step 1: Setting up databases..."
echo "========================================"
./setup_databases.sh

# Step 2: Setup Python environment
echo ""
echo "Step 2: Setting up Python environment..."
echo "========================================"

# Check if conda is available
if ! command -v conda &> /dev/null; then
    echo "ERROR: conda is not installed"
    echo "Please install Anaconda or Miniconda first"
    exit 1
fi

# Setup environment
if ./setup_env.sh; then
    echo "Environment setup complete"
else
    echo "Using existing environment"
fi

# Step 3: Activate environment and run benchmark
echo ""
echo "Step 3: Running benchmark..."
echo "========================================"

# Get conda base directory
CONDA_BASE=$(conda info --base)

# Activate conda environment
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate py310

# Run the benchmark
python run_all.py

echo ""
echo "========================================"
echo "Quick Start Complete!"
echo "========================================"
echo ""
echo "Check the results/ directory for:"
echo "  - benchmark_summary.csv"
echo "  - performance_comparison.png"
echo "  - Detailed results for each database"
echo ""
