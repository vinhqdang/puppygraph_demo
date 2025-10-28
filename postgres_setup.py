"""
PostgreSQL database setup and data loading.
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import Optional
import time
from config import DatabaseConfig


class PostgresSetup:
    """Setup PostgreSQL database and load banking data."""

    def __init__(self):
        """Initialize PostgreSQL setup."""
        self.config = DatabaseConfig()
        self.conn: Optional[psycopg2.extensions.connection] = None

    def create_database(self) -> None:
        """Create the banking database if it doesn't exist."""
        print("Creating PostgreSQL database...")

        # Connect to default postgres database
        conn = psycopg2.connect(
            host=self.config.POSTGRES_HOST,
            port=self.config.POSTGRES_PORT,
            database='postgres',
            user=self.config.POSTGRES_USER,
            password=self.config.POSTGRES_PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.config.POSTGRES_DB,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f"CREATE DATABASE {self.config.POSTGRES_DB}")
            print(f"Database '{self.config.POSTGRES_DB}' created successfully")
        else:
            print(f"Database '{self.config.POSTGRES_DB}' already exists")

        cursor.close()
        conn.close()

    def connect(self) -> None:
        """Connect to the banking database."""
        print("Connecting to PostgreSQL database...")
        self.conn = psycopg2.connect(
            host=self.config.POSTGRES_HOST,
            port=self.config.POSTGRES_PORT,
            database=self.config.POSTGRES_DB,
            user=self.config.POSTGRES_USER,
            password=self.config.POSTGRES_PASSWORD
        )
        print("Connected successfully")

    def create_schema(self) -> None:
        """Create database schema (tables and indexes)."""
        print("Creating database schema...")

        cursor = self.conn.cursor()

        # Drop existing tables
        cursor.execute("DROP TABLE IF EXISTS card_transactions CASCADE")
        cursor.execute("DROP TABLE IF EXISTS transactions CASCADE")
        cursor.execute("DROP TABLE IF EXISTS customers CASCADE")

        # Create customers table
        cursor.execute("""
            CREATE TABLE customers (
                customer_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                account_balance DECIMAL(15, 2),
                risk_score DECIMAL(5, 2),
                account_type VARCHAR(50),
                registration_date DATE
            )
        """)

        # Create transactions table
        cursor.execute("""
            CREATE TABLE transactions (
                transaction_id VARCHAR(50) PRIMARY KEY,
                from_customer_id VARCHAR(50) REFERENCES customers(customer_id),
                to_customer_id VARCHAR(50) REFERENCES customers(customer_id),
                amount DECIMAL(15, 2),
                transaction_date TIMESTAMP,
                transaction_type VARCHAR(50),
                status VARCHAR(50)
            )
        """)

        # Create card transactions table
        cursor.execute("""
            CREATE TABLE card_transactions (
                card_transaction_id VARCHAR(50) PRIMARY KEY,
                customer_id VARCHAR(50) REFERENCES customers(customer_id),
                merchant VARCHAR(255),
                amount DECIMAL(15, 2),
                transaction_date TIMESTAMP,
                card_type VARCHAR(50),
                merchant_category VARCHAR(50)
            )
        """)

        self.conn.commit()
        print("Schema created successfully")

    def create_indexes(self) -> None:
        """Create indexes for better query performance."""
        print("Creating indexes...")

        cursor = self.conn.cursor()

        # Indexes on transactions
        cursor.execute("CREATE INDEX idx_transactions_from ON transactions(from_customer_id)")
        cursor.execute("CREATE INDEX idx_transactions_to ON transactions(to_customer_id)")
        cursor.execute("CREATE INDEX idx_transactions_date ON transactions(transaction_date)")
        cursor.execute("CREATE INDEX idx_transactions_status ON transactions(status)")

        # Indexes on card transactions
        cursor.execute("CREATE INDEX idx_card_transactions_customer ON card_transactions(customer_id)")
        cursor.execute("CREATE INDEX idx_card_transactions_date ON card_transactions(transaction_date)")

        self.conn.commit()
        print("Indexes created successfully")

    def load_data(self, data_dir: str) -> None:
        """
        Load data from CSV files into PostgreSQL.

        Args:
            data_dir: Directory containing CSV files
        """
        print("Loading data into PostgreSQL...")

        # Load customers
        print("Loading customers...")
        customers_df = pd.read_csv(f"{data_dir}/customers.csv")
        cursor = self.conn.cursor()

        customers_data = [
            (row['customer_id'], row['name'], row['email'], row['account_balance'],
             row['risk_score'], row['account_type'], row['registration_date'])
            for _, row in customers_df.iterrows()
        ]

        execute_values(
            cursor,
            "INSERT INTO customers VALUES %s",
            customers_data,
            page_size=1000
        )
        self.conn.commit()
        print(f"Loaded {len(customers_df)} customers")

        # Load transactions
        print("Loading transactions...")
        transactions_df = pd.read_csv(f"{data_dir}/transactions.csv")

        transactions_data = [
            (row['transaction_id'], row['from_customer_id'], row['to_customer_id'],
             row['amount'], row['transaction_date'], row['transaction_type'], row['status'])
            for _, row in transactions_df.iterrows()
        ]

        execute_values(
            cursor,
            "INSERT INTO transactions VALUES %s",
            transactions_data,
            page_size=1000
        )
        self.conn.commit()
        print(f"Loaded {len(transactions_df)} transactions")

        # Load card transactions
        print("Loading card transactions...")
        card_transactions_df = pd.read_csv(f"{data_dir}/card_transactions.csv")

        card_data = [
            (row['card_transaction_id'], row['customer_id'], row['merchant'],
             row['amount'], row['transaction_date'], row['card_type'], row['merchant_category'])
            for _, row in card_transactions_df.iterrows()
        ]

        execute_values(
            cursor,
            "INSERT INTO card_transactions VALUES %s",
            card_data,
            page_size=1000
        )
        self.conn.commit()
        print(f"Loaded {len(card_transactions_df)} card transactions")

        cursor.close()
        print("Data loading completed successfully")

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            print("PostgreSQL connection closed")


def main():
    """Main function to setup PostgreSQL database."""
    print("=" * 60)
    print("PostgreSQL Database Setup")
    print("=" * 60)

    setup = PostgresSetup()

    try:
        start_time = time.time()

        # Create database
        setup.create_database()

        # Connect to database
        setup.connect()

        # Create schema
        setup.create_schema()

        # Load data
        setup.load_data(DatabaseConfig.DATA_DIR)

        # Create indexes
        setup.create_indexes()

        elapsed_time = time.time() - start_time

        print("\n" + "=" * 60)
        print(f"PostgreSQL setup completed in {elapsed_time:.2f} seconds")
        print("=" * 60)

    except Exception as e:
        print(f"Error during PostgreSQL setup: {e}")
        raise
    finally:
        setup.close()


if __name__ == "__main__":
    main()
