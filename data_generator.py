"""
Synthetic banking data generator.
Generates customer data, transactions, and card transactions.
"""
import os
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Tuple
from config import DatabaseConfig


class BankingDataGenerator:
    """Generate synthetic banking transaction data."""

    def __init__(self, num_customers: int, num_transactions: int, num_card_transactions: int):
        """
        Initialize the data generator.

        Args:
            num_customers: Number of customers to generate
            num_transactions: Number of bank transfers to generate
            num_card_transactions: Number of card transactions to generate
        """
        self.num_customers = num_customers
        self.num_transactions = num_transactions
        self.num_card_transactions = num_card_transactions
        random.seed(42)
        np.random.seed(42)

    def generate_customers(self) -> pd.DataFrame:
        """
        Generate customer data.

        Returns:
            DataFrame with customer information
        """
        print(f"Generating {self.num_customers} customers...")

        customers = []
        for i in range(self.num_customers):
            customer = {
                'customer_id': f'CUST_{i:06d}',
                'name': f'Customer_{i}',
                'email': f'customer_{i}@example.com',
                'account_balance': round(random.uniform(1000, 100000), 2),
                'risk_score': round(random.uniform(0, 100), 2),
                'account_type': random.choice(['savings', 'checking', 'business']),
                'registration_date': (datetime.now() - timedelta(days=random.randint(1, 1000))).strftime('%Y-%m-%d')
            }
            customers.append(customer)

        df = pd.DataFrame(customers)
        print(f"Generated {len(df)} customers")
        return df

    def generate_transactions(self, customers_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate bank transfer transactions.
        Creates a graph structure where customers transfer money to each other.

        Args:
            customers_df: DataFrame of customers

        Returns:
            DataFrame with transaction data
        """
        print(f"Generating {self.num_transactions} transactions...")

        customer_ids = customers_df['customer_id'].tolist()
        transactions = []

        # Create some high-frequency transferrers (hubs in the graph)
        hub_customers = random.sample(customer_ids, min(100, len(customer_ids) // 10))

        for i in range(self.num_transactions):
            # 30% of transactions involve hub customers
            if random.random() < 0.3 and hub_customers:
                from_customer = random.choice(hub_customers)
            else:
                from_customer = random.choice(customer_ids)

            # Ensure different sender and receiver
            to_customer = random.choice(customer_ids)
            while to_customer == from_customer:
                to_customer = random.choice(customer_ids)

            transaction = {
                'transaction_id': f'TXN_{i:08d}',
                'from_customer_id': from_customer,
                'to_customer_id': to_customer,
                'amount': round(random.uniform(10, 10000), 2),
                'transaction_date': (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d %H:%M:%S'),
                'transaction_type': random.choice(['transfer', 'payment', 'wire']),
                'status': random.choice(['completed'] * 95 + ['pending'] * 4 + ['failed'])
            }
            transactions.append(transaction)

        df = pd.DataFrame(transactions)
        print(f"Generated {len(df)} transactions")
        return df

    def generate_card_transactions(self, customers_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate card transaction data.

        Args:
            customers_df: DataFrame of customers

        Returns:
            DataFrame with card transaction data
        """
        print(f"Generating {self.num_card_transactions} card transactions...")

        customer_ids = customers_df['customer_id'].tolist()
        merchants = ['Amazon', 'Walmart', 'Target', 'Starbucks', 'Shell', 'McDonald\'s',
                    'Best Buy', 'Home Depot', 'Costco', 'Apple Store']

        card_transactions = []

        for i in range(self.num_card_transactions):
            transaction = {
                'card_transaction_id': f'CARD_{i:08d}',
                'customer_id': random.choice(customer_ids),
                'merchant': random.choice(merchants),
                'amount': round(random.uniform(5, 500), 2),
                'transaction_date': (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d %H:%M:%S'),
                'card_type': random.choice(['credit', 'debit']),
                'merchant_category': random.choice(['retail', 'food', 'gas', 'entertainment', 'travel'])
            }
            card_transactions.append(transaction)

        df = pd.DataFrame(card_transactions)
        print(f"Generated {len(df)} card transactions")
        return df

    def save_data(self, customers_df: pd.DataFrame, transactions_df: pd.DataFrame,
                  card_transactions_df: pd.DataFrame, output_dir: str) -> None:
        """
        Save generated data to CSV files.

        Args:
            customers_df: Customer data
            transactions_df: Transaction data
            card_transactions_df: Card transaction data
            output_dir: Directory to save CSV files
        """
        os.makedirs(output_dir, exist_ok=True)

        customers_file = os.path.join(output_dir, 'customers.csv')
        transactions_file = os.path.join(output_dir, 'transactions.csv')
        card_transactions_file = os.path.join(output_dir, 'card_transactions.csv')

        customers_df.to_csv(customers_file, index=False)
        transactions_df.to_csv(transactions_file, index=False)
        card_transactions_df.to_csv(card_transactions_file, index=False)

        print(f"\nData saved to:")
        print(f"  - {customers_file}")
        print(f"  - {transactions_file}")
        print(f"  - {card_transactions_file}")

    def generate_all(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Generate all banking data.

        Returns:
            Tuple of (customers, transactions, card_transactions) DataFrames
        """
        customers_df = self.generate_customers()
        transactions_df = self.generate_transactions(customers_df)
        card_transactions_df = self.generate_card_transactions(customers_df)

        return customers_df, transactions_df, card_transactions_df


def main():
    """Main function to generate banking data."""
    print("=" * 60)
    print("Banking Data Generator")
    print("=" * 60)

    generator = BankingDataGenerator(
        num_customers=DatabaseConfig.NUM_CUSTOMERS,
        num_transactions=DatabaseConfig.NUM_TRANSACTIONS,
        num_card_transactions=DatabaseConfig.NUM_CARD_TRANSACTIONS
    )

    customers_df, transactions_df, card_transactions_df = generator.generate_all()

    # Print statistics
    print("\n" + "=" * 60)
    print("Data Statistics:")
    print("=" * 60)
    print(f"Total Customers: {len(customers_df)}")
    print(f"Total Transactions: {len(transactions_df)}")
    print(f"Total Card Transactions: {len(card_transactions_df)}")
    print(f"Average transactions per customer: {len(transactions_df) / len(customers_df):.2f}")

    # Save data
    generator.save_data(customers_df, transactions_df, card_transactions_df, DatabaseConfig.DATA_DIR)

    print("\n" + "=" * 60)
    print("Data generation completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
