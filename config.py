"""
Configuration for database connections.
"""
import os


class DatabaseConfig:
    """Configuration for all database connections."""

    # PostgreSQL Configuration
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "banking_db")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

    # Neo4j Configuration
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

    # PuppyGraph Configuration
    PUPPYGRAPH_HOST = os.getenv("PUPPYGRAPH_HOST", "localhost")
    PUPPYGRAPH_PORT = os.getenv("PUPPYGRAPH_PORT", "8081")
    PUPPYGRAPH_GREMLIN_PORT = os.getenv("PUPPYGRAPH_GREMLIN_PORT", "8182")

    # Data Configuration
    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
    NUM_CUSTOMERS = 10000
    NUM_TRANSACTIONS = 100000
    NUM_CARD_TRANSACTIONS = 50000

    @classmethod
    def get_postgres_connection_string(cls):
        """Get PostgreSQL connection string."""
        return f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASSWORD}@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"
