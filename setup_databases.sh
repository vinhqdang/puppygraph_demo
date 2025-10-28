#!/bin/bash

# Setup script for PostgreSQL and Neo4j using Docker
# This script checks for Docker, installs if needed, and starts the databases

set -e  # Exit on error

echo "========================================"
echo "Database Setup for Graph Benchmark"
echo "========================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Check if Docker is installed
check_docker() {
    if command -v docker &> /dev/null; then
        print_success "Docker is installed"
        docker --version
        return 0
    else
        print_error "Docker is not installed"
        return 1
    fi
}

# Check if Docker Compose is available
check_docker_compose() {
    if docker compose version &> /dev/null; then
        print_success "Docker Compose is available"
        docker compose version
        return 0
    elif command -v docker-compose &> /dev/null; then
        print_success "Docker Compose (standalone) is available"
        docker-compose --version
        return 0
    else
        print_error "Docker Compose is not available"
        return 1
    fi
}

# Install Docker on WSL/Linux
install_docker() {
    print_info "Installing Docker..."

    # Update package list
    sudo apt-get update

    # Install prerequisites
    sudo apt-get install -y \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg \
        lsb-release

    # Add Docker's official GPG key
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

    # Set up the stable repository
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    # Install Docker Engine
    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

    # Add current user to docker group
    sudo usermod -aG docker $USER

    print_success "Docker installed successfully"
    print_info "You may need to log out and log back in for group changes to take effect"
}

# Start Docker service
start_docker_service() {
    if ! sudo service docker status &> /dev/null; then
        print_info "Starting Docker service..."
        sudo service docker start
        sleep 2
    fi

    if sudo service docker status &> /dev/null; then
        print_success "Docker service is running"
    else
        print_error "Failed to start Docker service"
        exit 1
    fi
}

# Main installation flow
echo "Step 1: Checking Docker installation..."
echo "========================================="
if ! check_docker; then
    read -p "Would you like to install Docker? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        install_docker
    else
        print_error "Docker is required. Exiting."
        exit 1
    fi
fi

echo ""
echo "Step 2: Checking Docker Compose..."
echo "========================================="
check_docker_compose || {
    print_error "Docker Compose is required but not found"
    exit 1
}

echo ""
echo "Step 3: Starting Docker service..."
echo "========================================="
start_docker_service

echo ""
echo "Step 4: Starting databases with Docker Compose..."
echo "========================================="

# Check if containers are already running
if docker ps | grep -q "puppygraph_postgres\|puppygraph_neo4j"; then
    print_info "Some containers are already running"
    read -p "Would you like to restart them? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_info "Stopping existing containers..."
        docker compose down
    fi
fi

# Start containers
print_info "Starting PostgreSQL and Neo4j containers..."
docker compose up -d

# Wait for services to be healthy
echo ""
echo "Step 5: Waiting for databases to be ready..."
echo "========================================="

print_info "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if docker exec puppygraph_postgres pg_isready -U postgres &> /dev/null; then
        print_success "PostgreSQL is ready!"
        break
    fi
    echo -n "."
    sleep 1
done

echo ""
print_info "Waiting for Neo4j to be ready..."
for i in {1..30}; do
    if docker exec puppygraph_neo4j wget --no-verbose --tries=1 --spider http://localhost:7474 &> /dev/null; then
        print_success "Neo4j is ready!"
        break
    fi
    echo -n "."
    sleep 1
done

echo ""
echo "========================================"
echo "Database Setup Complete!"
echo "========================================"
echo ""
print_success "PostgreSQL is running on localhost:5432"
echo "  - Username: postgres"
echo "  - Password: postgres"
echo "  - Database: banking_db"
echo ""
print_success "Neo4j is running on localhost:7687 (Bolt) and localhost:7474 (HTTP)"
echo "  - Username: neo4j"
echo "  - Password: password"
echo "  - Web UI: http://localhost:7474"
echo ""
print_success "PuppyGraph is running on localhost:8081 (HTTP) and localhost:8182 (Gremlin)"
echo "  - Password: puppygraph123"
echo ""
echo "To stop the databases:"
echo "  docker compose down"
echo ""
echo "To view logs:"
echo "  docker compose logs -f"
echo ""
echo "To restart the databases:"
echo "  docker compose restart"
echo ""
print_info "To run the complete benchmark (automatic setup + benchmark):"
echo "  ./run_complete_benchmark.sh"
echo ""
print_info "Or manually:"
echo "  conda activate py310"
echo "  python run_all.py"
echo ""
