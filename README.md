# Graph Database Performance Benchmark: PuppyGraph vs Neo4j vs PostgreSQL

This project provides a comprehensive performance comparison between three different approaches for querying graph data:
1. **PuppyGraph** - Query-time graph engine that reads data on-the-fly
2. **Neo4j** - Traditional native graph database
3. **PostgreSQL** - Relational database with graph queries

## Overview

The benchmark focuses on **2-hop graph feature aggregation** in a banking transaction network. For a given customer A who transfers money to customer B, we calculate aggregated features of all customers that B transfers money to (2-hop neighbors).

## Use Case: Banking Transaction Network

### Data Model
- **Customers**: Banking customers with account information
- **Transactions**: Money transfers between customers (creates graph edges)
- **Card Transactions**: Credit/debit card purchases by customers

### 2-Hop Graph Features
For each customer, we calculate:
- Number of unique 2-hop receivers
- Number of 2-hop transactions
- Average/Total/Min/Max transaction amounts at 2-hop
- Average risk scores of 2-hop receivers
- Average account balances of 2-hop receivers

## Project Structure

```
puppygraph_demo/
├── config.py                      # Configuration for all databases
├── data_generator.py              # Synthetic banking data generator
├── postgres_setup.py              # PostgreSQL database setup
├── neo4j_setup.py                # Neo4j database setup
├── puppygraph_setup.py           # PuppyGraph schema configuration
├── queries.py                    # Query implementations for all three systems
├── benchmark.py                  # Performance benchmarking script
├── run_all.py                   # Complete pipeline orchestrator
├── run_complete_benchmark.sh    # ONE COMMAND: Start DBs + Run + Cleanup
├── setup_databases.sh           # Start all 3 databases with Docker
├── setup_env.sh                # Conda environment setup
├── quick_start.sh              # Legacy quick start script
├── docker-compose.yml           # Docker config for all 3 databases
├── requirements.txt             # Python dependencies
├── puppygraph_schema.json      # PuppyGraph schema (generated)
├── data/                       # Generated CSV files
│   ├── customers.csv
│   ├── transactions.csv
│   └── card_transactions.csv
└── results/                    # Benchmark results
    ├── benchmark_summary.csv
    ├── performance_comparison.png
    └── *_detailed_results.csv
```

## Architecture & Implementation

### Data Generation
The `BankingDataGenerator` class creates realistic synthetic banking data:
- Generates 10,000 customers with account balances and risk scores
- Creates 100,000 transactions with hub customers (high-frequency transferrers)
- Generates 50,000 card transactions across various merchants

### Query Implementations

#### PostgreSQL Approach
Uses Common Table Expressions (CTEs) to traverse the graph:
```sql
WITH first_hop AS (
    SELECT DISTINCT to_customer_id FROM transactions
    WHERE from_customer_id = ? AND status = 'completed'
),
second_hop AS (
    SELECT t.to_customer_id, t.amount, ...
    FROM transactions t
    INNER JOIN first_hop fh ON t.from_customer_id = fh.to_customer_id
    WHERE t.status = 'completed'
)
SELECT COUNT(...), AVG(...), SUM(...) FROM second_hop
```

#### Neo4j Approach
Uses Cypher pattern matching:
```cypher
MATCH (source:Customer)-[t1:TRANSFERRED]->(hop1:Customer)
WHERE t1.status = 'completed'
WITH DISTINCT hop1
MATCH (hop1)-[t2:TRANSFERRED]->(hop2:Customer)
WHERE t2.status = 'completed'
RETURN COUNT(...), AVG(...), SUM(...)
```

#### PuppyGraph Approach
Uses Gremlin graph traversal:
```gremlin
g.V().has('Customer', 'customer_id', customer_id)
  .out('TRANSFERRED').has('status', 'completed')
  .dedup()
  .out('TRANSFERRED').has('status', 'completed')
  .fold()
  .project('num_unique_receivers', 'avg_amount', ...)
  .by(__.unfold().dedup().count())
  .by(__.unfold().values('amount').mean())
```

## Quick Start (Easiest Way)

**Single Command to Run Everything:**

```bash
# Activate conda environment first
conda activate py310

# Start all 3 databases, run benchmark, and cleanup
./run_complete_benchmark.sh
```

This automated script will:
1. Start PostgreSQL, Neo4j, and PuppyGraph using Docker
2. Wait for all databases to be ready
3. Generate synthetic banking data
4. Setup databases and load data
5. Run performance benchmark on all three systems
6. Stop all databases when complete

**⚠️ IMPORTANT - Manual Step Required:**
PuppyGraph requires manual schema loading via the web UI (http://localhost:8081). The script will pause and prompt you to load the schema. See the **[PuppyGraph Manual Schema Loading](#️-important-puppygraph-manual-schema-loading)** section for detailed instructions.

**Note:** Make sure you have activated the conda environment (py310) before running the script.

**Alternative (step-by-step):**

```bash
# 1. Start all databases
./setup_databases.sh

# 2. Setup Python environment
conda activate py310

# 3. Run the complete benchmark pipeline
python run_all.py
```

## Prerequisites

### Software Requirements
1. **Python 3.10** (using conda environment)
2. **Docker** - All three databases (PostgreSQL, Neo4j, PuppyGraph) run in Docker containers

## Database Setup

### Automated Docker Setup (RECOMMENDED)

All three databases run in Docker containers for easy setup:

```bash
# Automated setup - starts all databases
./setup_databases.sh

# Or manually with docker compose
docker compose up -d
```

The script will:
- Check if Docker is installed (and offer to install it if not)
- Start PostgreSQL, Neo4j, and PuppyGraph containers
- Wait for all databases to be ready
- Display connection information

**Docker Management Commands:**
```bash
# Start all databases
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f

# Stop databases (keeps data)
docker compose down

# Stop and remove all data
docker compose down -v

# Restart databases
docker compose restart
```

**Connection Details:**
- **PostgreSQL**: `localhost:5432`
  - Container: graph_benchmark_postgres
  - User: postgres
  - Password: postgres
  - Database: banking_db

- **Neo4j**:
  - Container: graph_benchmark_neo4j
  - Bolt: `localhost:7687` (user: neo4j, password: password)
  - Web UI: `http://localhost:7474`

- **PuppyGraph**:
  - Container: graph_benchmark_puppygraph
  - HTTP API: `localhost:8081`
  - Gremlin: `localhost:8182`
  - Password: puppygraph123
  - **Web UI**: `http://localhost:8081` (for schema loading)

### ⚠️ IMPORTANT: PuppyGraph Manual Schema Loading

PuppyGraph requires manual schema loading via the web UI. This is a **required step** before running benchmarks.

**Step-by-Step Instructions:**

1. **Ensure containers are running:**
   ```bash
   docker compose up -d
   # Wait 10-15 seconds for PuppyGraph to be fully ready
   ```

2. **Open PuppyGraph Web UI:**
   - Navigate to: `http://localhost:8081`
   - Login with password: `puppygraph123`

3. **Load the schema:**
   - The schema file is located at: `puppygraph_schema.json`
   - In the web UI, navigate to the **Schema** or **Configuration** section
   - Upload or paste the contents of `puppygraph_schema.json`
   - Click **Save** or **Apply** to load the schema

4. **Verify schema is loaded:**
   - The schema connects to PostgreSQL database running in Docker
   - Check that the connection is successful (you should see "Customer" vertex and "TRANSFERRED" edge)
   - PuppyGraph will query data directly from PostgreSQL via JDBC

**Schema Details:**
- **Catalog**: PostgreSQL database (`postgres:5432/banking_db`)
- **Vertex**: Customer (with attributes: name, email, account_balance, risk_score, etc.)
- **Edge**: TRANSFERRED (connecting Customer → Customer with transaction details)

**Note:** The schema uses the Docker service name `postgres` to connect to the PostgreSQL container. This is correct for Docker networking.

### Option 2: Manual Installation

#### PostgreSQL
```bash
# Install PostgreSQL (Ubuntu/Debian)
sudo apt-get install postgresql postgresql-contrib

# Start PostgreSQL
sudo service postgresql start

# Set password for postgres user
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres';"
```

#### Neo4j
```bash
# Download and install Neo4j Community Edition
# https://neo4j.com/download/

# Start Neo4j
neo4j start

# Set password via web UI at http://localhost:7474
# Default: neo4j/neo4j (change to neo4j/password)
```

#### PuppyGraph
```bash
# Follow PuppyGraph installation instructions
# Ensure it's running on:
# - HTTP API: localhost:8081
# - Gremlin: localhost:8182
```

## Installation & Setup

### Step 1: Create Conda Environment
```bash
# Create conda environment with Python 3.10
conda create -n py310 python=3.10 -y
conda activate py310

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Configure Database Connections
Edit `config.py` if your databases are not running with default settings:
```python
# PostgreSQL
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"

# Neo4j
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# PuppyGraph
PUPPYGRAPH_HOST = "localhost"
PUPPYGRAPH_PORT = "8081"
PUPPYGRAPH_GREMLIN_PORT = "8182"
```

## Usage

### Option 1: Run Complete Pipeline (Recommended)
Execute everything with a single command:
```bash
conda activate py310
python run_all.py
```

This will:
1. Generate synthetic banking data
2. Setup PostgreSQL database and load data
3. Setup Neo4j database and load data
4. Configure PuppyGraph schema
5. Run performance benchmarks on all three systems
6. Generate comparison reports and visualizations

### Option 2: Run Individual Steps

#### Step 1: Generate Data
```bash
python data_generator.py
```
Creates CSV files in `data/` directory with customers, transactions, and card transactions.

#### Step 2: Setup Databases
```bash
# PostgreSQL
python postgres_setup.py

# Neo4j
python neo4j_setup.py

# PuppyGraph
python puppygraph_setup.py
```

#### Step 3: Run Benchmark
```bash
python benchmark.py
```

### Option 3: Skip Certain Steps
```bash
# Skip data generation (use existing data)
python run_all.py --skip-data

# Skip database setup (use existing databases)
python run_all.py --skip-setup

# Run only benchmark
python run_all.py --skip-data --skip-setup
```

## Benchmark Results

The benchmark script will generate:

### 1. Console Output
```
================================================================================
PERFORMANCE COMPARISON REPORT
================================================================================

Database         Single Query Time (s)  Avg Query Time (s)  Queries/Second  Total Batch Time (s)
PostgreSQL       0.0234                 0.0245              40.82           2.45
Neo4j            0.0156                 0.0168              59.52           1.68
PuppyGraph       0.0089                 0.0095              105.26          0.95

================================================================================
SPEEDUP ANALYSIS (relative to PostgreSQL)
================================================================================
Neo4j speedup: 1.46x
PuppyGraph speedup: 2.58x
```

### 2. CSV Files (in `results/` directory)
- `benchmark_summary.csv` - Summary statistics for all databases
- `postgres_detailed_results.csv` - Detailed results for each query
- `neo4j_detailed_results.csv` - Detailed results for each query
- `puppygraph_detailed_results.csv` - Detailed results for each query

### 3. Visualization
- `performance_comparison.png` - Bar charts comparing average query time and queries per second

## Performance Insights

### Expected Results
Based on the design patterns:

1. **PostgreSQL**
   - Strengths: Robust, reliable, familiar SQL
   - Limitations: Multiple self-joins for graph traversal are expensive
   - Best for: Simple 1-hop queries or when you already use PostgreSQL

2. **Neo4j**
   - Strengths: Native graph storage, optimized for graph traversals
   - Optimizations: Index-free adjacency for fast neighbor lookups
   - Best for: Complex multi-hop queries, graph algorithms

3. **PuppyGraph**
   - Strengths: Query-time processing, no data duplication, fast setup
   - Optimizations: Reads directly from source files with graph semantics
   - Best for: Rapid prototyping, when you want graph capabilities without ETL

### Factors Affecting Performance
- Data size and graph density
- Number of hops in traversal
- Aggregation complexity
- Hardware specifications (CPU, RAM, SSD)
- Database tuning and indexing

## Customization

### Adjust Data Size
Edit `config.py`:
```python
NUM_CUSTOMERS = 10000          # Number of customers
NUM_TRANSACTIONS = 100000      # Number of transactions
NUM_CARD_TRANSACTIONS = 50000  # Number of card transactions
```

### Adjust Test Sample Size
Edit `benchmark.py` or pass parameter:
```python
benchmark.run_full_benchmark(num_test_customers=100)  # Test on 100 customers
```

### Add Custom Queries
Extend the `queries.py` file with additional methods in each query class:
```python
class PostgresQueries:
    def your_custom_query(self, params):
        # Implementation
        pass
```

## Troubleshooting

### PostgreSQL Connection Issues
```bash
# Check container status
docker ps --filter "name=graph_benchmark_postgres"

# Check logs
docker logs graph_benchmark_postgres

# Verify connection
docker exec graph_benchmark_postgres psql -U postgres -c "SELECT version();"
```

### Neo4j Connection Issues
```bash
# Check container status
docker ps --filter "name=graph_benchmark_neo4j"

# Check logs
docker logs graph_benchmark_neo4j

# Test connection
curl http://localhost:7474
```

### PuppyGraph Connection Issues
```bash
# Check container status
docker ps --filter "name=graph_benchmark_puppygraph"

# Check logs
docker logs graph_benchmark_puppygraph

# Verify PuppyGraph is running
curl http://localhost:8081
```

**Important Note About PuppyGraph Setup:**
The benchmark script now uses HTTP endpoint verification instead of Gremlin queries during initial connection checks. This prevents hanging when PuppyGraph doesn't have a schema loaded yet. The Gremlin client connections include proper timeouts (30 seconds) to prevent indefinite blocking.

If you experience hanging during PuppyGraph connection:
1. Ensure PuppyGraph container is healthy: `docker ps`
2. Check the HTTP endpoint is accessible: `curl http://localhost:8081`
3. The setup script will skip query tests if no schema is loaded - this is expected behavior

### Python Dependencies Issues
```bash
# Reinstall dependencies
conda activate py310
pip install --upgrade -r requirements.txt
```

## Development Notes

### Design Patterns Used
- **Factory Pattern**: Query class instantiation
- **Strategy Pattern**: Different query implementations for each database
- **Template Method**: Benchmark execution flow
- **Dependency Injection**: Configuration management

### Code Organization
- `config.py`: Centralized configuration
- `*_setup.py`: Database setup (separation of concerns)
- `queries.py`: Query implementations (single responsibility)
- `benchmark.py`: Performance measurement (observer pattern)
- `run_all.py`: Orchestration (facade pattern)

### Testing Approach
Each module can be run independently for testing:
```bash
python data_generator.py  # Test data generation
python postgres_setup.py  # Test PostgreSQL setup
python neo4j_setup.py    # Test Neo4j setup
python puppygraph_setup.py  # Test PuppyGraph setup
```

## Future Enhancements

1. **Additional Queries**
   - 3-hop and N-hop traversals
   - PageRank and centrality metrics
   - Community detection
   - Shortest path calculations

2. **More Databases**
   - Amazon Neptune
   - TigerGraph
   - JanusGraph
   - ArangoDB

3. **Advanced Benchmarks**
   - Concurrent query execution
   - Write performance testing
   - Memory usage profiling
   - Scalability testing with larger datasets

4. **Optimization**
   - Query optimization for each database
   - Index tuning
   - Parallel query execution
   - Caching strategies

## License

MIT License - See LICENSE file for details

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

## References

- [PuppyGraph Documentation](https://docs.puppygraph.com)
- [Neo4j Documentation](https://neo4j.com/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Gremlin Documentation](https://tinkerpop.apache.org/docs/current/reference/)
- [Cypher Query Language](https://neo4j.com/developer/cypher/)

## Contact

For questions or issues, please open an issue on GitHub.
