"""
Neo4j database setup and data loading.
"""
import pandas as pd
from neo4j import GraphDatabase
from typing import Optional
import time
from config import DatabaseConfig


class Neo4jSetup:
    """Setup Neo4j database and load banking data."""

    def __init__(self):
        """Initialize Neo4j setup."""
        self.config = DatabaseConfig()
        self.driver: Optional[GraphDatabase.driver] = None

    def connect(self) -> None:
        """Connect to Neo4j database."""
        print("Connecting to Neo4j database...")
        self.driver = GraphDatabase.driver(
            self.config.NEO4J_URI,
            auth=(self.config.NEO4J_USER, self.config.NEO4J_PASSWORD)
        )
        # Verify connection
        self.driver.verify_connectivity()
        print("Connected successfully")

    def clear_database(self) -> None:
        """Clear all data from the database."""
        print("Clearing existing data...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("Database cleared")

    def create_constraints(self) -> None:
        """Create constraints and indexes for better performance."""
        print("Creating constraints and indexes...")

        with self.driver.session() as session:
            # Create uniqueness constraints
            try:
                session.run("CREATE CONSTRAINT customer_id_unique IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE")
                session.run("CREATE CONSTRAINT transaction_id_unique IF NOT EXISTS FOR (t:Transaction) REQUIRE t.transaction_id IS UNIQUE")
                session.run("CREATE CONSTRAINT card_transaction_id_unique IF NOT EXISTS FOR (ct:CardTransaction) REQUIRE ct.card_transaction_id IS UNIQUE")
            except Exception as e:
                print(f"Note: Constraints may already exist: {e}")

            # Create indexes
            try:
                session.run("CREATE INDEX customer_email IF NOT EXISTS FOR (c:Customer) ON (c.email)")
                session.run("CREATE INDEX transaction_date IF NOT EXISTS FOR (t:Transaction) ON (t.transaction_date)")
                session.run("CREATE INDEX card_transaction_date IF NOT EXISTS FOR (ct:CardTransaction) ON (ct.transaction_date)")
            except Exception as e:
                print(f"Note: Indexes may already exist: {e}")

        print("Constraints and indexes created")

    def load_customers(self, customers_df: pd.DataFrame) -> None:
        """
        Load customers into Neo4j.

        Args:
            customers_df: DataFrame containing customer data
        """
        print(f"Loading {len(customers_df)} customers...")

        with self.driver.session() as session:
            # Batch insert customers
            batch_size = 1000
            for i in range(0, len(customers_df), batch_size):
                batch = customers_df.iloc[i:i + batch_size]
                customers_list = batch.to_dict('records')

                session.run("""
                    UNWIND $customers AS customer
                    CREATE (c:Customer {
                        customer_id: customer.customer_id,
                        name: customer.name,
                        email: customer.email,
                        account_balance: customer.account_balance,
                        risk_score: customer.risk_score,
                        account_type: customer.account_type,
                        registration_date: customer.registration_date
                    })
                """, customers=customers_list)

                if (i + batch_size) % 5000 == 0:
                    print(f"  Loaded {min(i + batch_size, len(customers_df))} customers...")

        print(f"Loaded {len(customers_df)} customers successfully")

    def load_transactions(self, transactions_df: pd.DataFrame) -> None:
        """
        Load transactions into Neo4j as relationships.

        Args:
            transactions_df: DataFrame containing transaction data
        """
        print(f"Loading {len(transactions_df)} transactions...")

        with self.driver.session() as session:
            # Batch insert transactions
            batch_size = 1000
            for i in range(0, len(transactions_df), batch_size):
                batch = transactions_df.iloc[i:i + batch_size]
                transactions_list = batch.to_dict('records')

                session.run("""
                    UNWIND $transactions AS txn
                    MATCH (from:Customer {customer_id: txn.from_customer_id})
                    MATCH (to:Customer {customer_id: txn.to_customer_id})
                    CREATE (from)-[t:TRANSFERRED {
                        transaction_id: txn.transaction_id,
                        amount: txn.amount,
                        transaction_date: txn.transaction_date,
                        transaction_type: txn.transaction_type,
                        status: txn.status
                    }]->(to)
                """, transactions=transactions_list)

                if (i + batch_size) % 5000 == 0:
                    print(f"  Loaded {min(i + batch_size, len(transactions_df))} transactions...")

        print(f"Loaded {len(transactions_df)} transactions successfully")

    def load_card_transactions(self, card_transactions_df: pd.DataFrame) -> None:
        """
        Load card transactions into Neo4j.

        Args:
            card_transactions_df: DataFrame containing card transaction data
        """
        print(f"Loading {len(card_transactions_df)} card transactions...")

        with self.driver.session() as session:
            # Batch insert card transactions
            batch_size = 1000
            for i in range(0, len(card_transactions_df), batch_size):
                batch = card_transactions_df.iloc[i:i + batch_size]
                card_txns_list = batch.to_dict('records')

                session.run("""
                    UNWIND $card_txns AS card_txn
                    MATCH (c:Customer {customer_id: card_txn.customer_id})
                    CREATE (ct:CardTransaction {
                        card_transaction_id: card_txn.card_transaction_id,
                        merchant: card_txn.merchant,
                        amount: card_txn.amount,
                        transaction_date: card_txn.transaction_date,
                        card_type: card_txn.card_type,
                        merchant_category: card_txn.merchant_category
                    })
                    CREATE (c)-[:MADE_CARD_TRANSACTION]->(ct)
                """, card_txns=card_txns_list)

                if (i + batch_size) % 5000 == 0:
                    print(f"  Loaded {min(i + batch_size, len(card_transactions_df))} card transactions...")

        print(f"Loaded {len(card_transactions_df)} card transactions successfully")

    def close(self) -> None:
        """Close Neo4j connection."""
        if self.driver:
            self.driver.close()
            print("Neo4j connection closed")


def main():
    """Main function to setup Neo4j database."""
    print("=" * 60)
    print("Neo4j Database Setup")
    print("=" * 60)

    setup = Neo4jSetup()

    try:
        start_time = time.time()

        # Connect to Neo4j
        setup.connect()

        # Clear existing data
        setup.clear_database()

        # Create constraints and indexes
        setup.create_constraints()

        # Load data from CSV files
        print("\nLoading data from CSV files...")
        customers_df = pd.read_csv(f"{DatabaseConfig.DATA_DIR}/customers.csv")
        transactions_df = pd.read_csv(f"{DatabaseConfig.DATA_DIR}/transactions.csv")
        card_transactions_df = pd.read_csv(f"{DatabaseConfig.DATA_DIR}/card_transactions.csv")

        setup.load_customers(customers_df)
        setup.load_transactions(transactions_df)
        setup.load_card_transactions(card_transactions_df)

        elapsed_time = time.time() - start_time

        print("\n" + "=" * 60)
        print(f"Neo4j setup completed in {elapsed_time:.2f} seconds")
        print("=" * 60)

    except Exception as e:
        print(f"Error during Neo4j setup: {e}")
        raise
    finally:
        setup.close()


if __name__ == "__main__":
    main()
