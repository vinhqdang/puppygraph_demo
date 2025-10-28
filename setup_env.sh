#!/bin/bash

# Setup script for Graph Database Benchmark
# This script creates the conda environment and installs all dependencies

set -e  # Exit on error

echo "========================================"
echo "Graph Database Benchmark - Environment Setup"
echo "========================================"

# Check if conda is installed
if ! command -v conda &> /dev/null; then
    echo "ERROR: conda is not installed or not in PATH"
    echo "Please install Anaconda or Miniconda first"
    exit 1
fi

# Environment name
ENV_NAME="py310"

# Check if environment already exists
if conda env list | grep -q "^${ENV_NAME} "; then
    echo "Environment '${ENV_NAME}' already exists"
    read -p "Do you want to recreate it? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Removing existing environment..."
        conda env remove -n ${ENV_NAME} -y
    else
        echo "Using existing environment"
        conda activate ${ENV_NAME}
        echo "Installing/updating dependencies..."
        pip install -r requirements.txt
        echo "Setup completed!"
        exit 0
    fi
fi

# Create conda environment
echo "Creating conda environment '${ENV_NAME}' with Python 3.10..."
conda create -n ${ENV_NAME} python=3.10 -y

# Activate environment
echo "Activating environment..."
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate ${ENV_NAME}

# Install dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

echo ""
echo "========================================"
echo "Setup completed successfully!"
echo "========================================"
echo ""
echo "To activate the environment, run:"
echo "  conda activate ${ENV_NAME}"
echo ""
echo "To run the complete benchmark, run:"
echo "  python run_all.py"
echo ""
