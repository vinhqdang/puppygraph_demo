"""
Query implementations for all three database systems.
Implements 2-hop graph feature calculations.
"""
import time
from typing import List, Dict, Any
import psycopg2
from neo4j import GraphDatabase
from gremlin_python.driver import client, serializer
from config import DatabaseConfig


class QueryBenchmark:
    """Base class for query benchmarking."""

    def __init__(self):
        """Initialize benchmark."""
        self.config = DatabaseConfig()

    def measure_query_time(self, query_func, *args, **kwargs) -> tuple:
        """
        Measure query execution time.

        Args:
            query_func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Tuple of (result, execution_time_in_seconds)
        """
        start_time = time.time()
        result = query_func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        return result, elapsed_time


class PostgresQueries(QueryBenchmark):
    """PostgreSQL query implementations."""

    def __init__(self):
        """Initialize PostgreSQL connection."""
        super().__init__()
        self.conn = psycopg2.connect(
            host=self.config.POSTGRES_HOST,
            port=self.config.POSTGRES_PORT,
            database=self.config.POSTGRES_DB,
            user=self.config.POSTGRES_USER,
            password=self.config.POSTGRES_PASSWORD
        )

    def two_hop_aggregation(self, customer_id: str) -> Dict[str, Any]:
        """
        Calculate 2-hop aggregation features for a customer.
        For customer A who transfers to B, calculate aggregated features of B's receivers.

        Args:
            customer_id: Customer ID to analyze

        Returns:
            Dictionary with aggregated features
        """
        cursor = self.conn.cursor()

        query = """
        WITH first_hop AS (
            -- Get all customers that the target customer transferred money to (1-hop)
            SELECT DISTINCT t1.to_customer_id
            FROM transactions t1
            WHERE t1.from_customer_id = %s
              AND t1.status = 'completed'
        ),
        second_hop AS (
            -- Get all customers that the 1-hop customers transferred money to (2-hop)
            SELECT
                t2.to_customer_id,
                t2.amount,
                t2.transaction_date
            FROM transactions t2
            INNER JOIN first_hop fh ON t2.from_customer_id = fh.to_customer_id
            WHERE t2.status = 'completed'
        )
        SELECT
            COUNT(DISTINCT sh.to_customer_id) as num_unique_2hop_receivers,
            COUNT(*) as num_2hop_transactions,
            COALESCE(AVG(sh.amount), 0) as avg_2hop_transaction_amount,
            COALESCE(SUM(sh.amount), 0) as total_2hop_transaction_amount,
            COALESCE(MAX(sh.amount), 0) as max_2hop_transaction_amount,
            COALESCE(MIN(sh.amount), 0) as min_2hop_transaction_amount,
            COALESCE(AVG(c.risk_score), 0) as avg_2hop_receiver_risk_score,
            COALESCE(AVG(c.account_balance), 0) as avg_2hop_receiver_balance
        FROM second_hop sh
        LEFT JOIN customers c ON sh.to_customer_id = c.customer_id
        """

        cursor.execute(query, (customer_id,))
        result = cursor.fetchone()

        features = {
            'num_unique_2hop_receivers': result[0],
            'num_2hop_transactions': result[1],
            'avg_2hop_transaction_amount': float(result[2]),
            'total_2hop_transaction_amount': float(result[3]),
            'max_2hop_transaction_amount': float(result[4]),
            'min_2hop_transaction_amount': float(result[5]),
            'avg_2hop_receiver_risk_score': float(result[6]),
            'avg_2hop_receiver_balance': float(result[7])
        }

        cursor.close()
        return features

    def batch_two_hop_aggregation(self, customer_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Calculate 2-hop aggregation for multiple customers.

        Args:
            customer_ids: List of customer IDs

        Returns:
            List of feature dictionaries
        """
        results = []
        for customer_id in customer_ids:
            features = self.two_hop_aggregation(customer_id)
            features['customer_id'] = customer_id
            results.append(features)
        return results

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


class Neo4jQueries(QueryBenchmark):
    """Neo4j query implementations."""

    def __init__(self):
        """Initialize Neo4j connection."""
        super().__init__()
        self.driver = GraphDatabase.driver(
            self.config.NEO4J_URI,
            auth=(self.config.NEO4J_USER, self.config.NEO4J_PASSWORD)
        )

    def two_hop_aggregation(self, customer_id: str) -> Dict[str, Any]:
        """
        Calculate 2-hop aggregation features for a customer using Cypher.

        Args:
            customer_id: Customer ID to analyze

        Returns:
            Dictionary with aggregated features
        """
        with self.driver.session() as session:
            query = """
            MATCH (source:Customer {customer_id: $customer_id})-[t1:TRANSFERRED]->(hop1:Customer)
            WHERE t1.status = 'completed'
            WITH DISTINCT hop1
            MATCH (hop1)-[t2:TRANSFERRED]->(hop2:Customer)
            WHERE t2.status = 'completed'
            RETURN
                COUNT(DISTINCT hop2) as num_unique_2hop_receivers,
                COUNT(t2) as num_2hop_transactions,
                AVG(t2.amount) as avg_2hop_transaction_amount,
                SUM(t2.amount) as total_2hop_transaction_amount,
                MAX(t2.amount) as max_2hop_transaction_amount,
                MIN(t2.amount) as min_2hop_transaction_amount,
                AVG(hop2.risk_score) as avg_2hop_receiver_risk_score,
                AVG(hop2.account_balance) as avg_2hop_receiver_balance
            """

            result = session.run(query, customer_id=customer_id)
            record = result.single()

            if record is None:
                return {
                    'num_unique_2hop_receivers': 0,
                    'num_2hop_transactions': 0,
                    'avg_2hop_transaction_amount': 0.0,
                    'total_2hop_transaction_amount': 0.0,
                    'max_2hop_transaction_amount': 0.0,
                    'min_2hop_transaction_amount': 0.0,
                    'avg_2hop_receiver_risk_score': 0.0,
                    'avg_2hop_receiver_balance': 0.0
                }

            features = {
                'num_unique_2hop_receivers': record['num_unique_2hop_receivers'],
                'num_2hop_transactions': record['num_2hop_transactions'],
                'avg_2hop_transaction_amount': float(record['avg_2hop_transaction_amount'] or 0),
                'total_2hop_transaction_amount': float(record['total_2hop_transaction_amount'] or 0),
                'max_2hop_transaction_amount': float(record['max_2hop_transaction_amount'] or 0),
                'min_2hop_transaction_amount': float(record['min_2hop_transaction_amount'] or 0),
                'avg_2hop_receiver_risk_score': float(record['avg_2hop_receiver_risk_score'] or 0),
                'avg_2hop_receiver_balance': float(record['avg_2hop_receiver_balance'] or 0)
            }

            return features

    def batch_two_hop_aggregation(self, customer_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Calculate 2-hop aggregation for multiple customers.

        Args:
            customer_ids: List of customer IDs

        Returns:
            List of feature dictionaries
        """
        results = []
        for customer_id in customer_ids:
            features = self.two_hop_aggregation(customer_id)
            features['customer_id'] = customer_id
            results.append(features)
        return results

    def close(self):
        """Close database connection."""
        if self.driver:
            self.driver.close()


class PuppyGraphQueries(QueryBenchmark):
    """PuppyGraph query implementations using Gremlin."""

    def __init__(self):
        """Initialize PuppyGraph connection."""
        super().__init__()
        gremlin_url = f"ws://{self.config.PUPPYGRAPH_HOST}:{self.config.PUPPYGRAPH_GREMLIN_PORT}/gremlin"
        self.client = client.Client(
            gremlin_url,
            'g',
            message_serializer=serializer.GraphSONSerializersV2d0(),
            pool_size=4,
            max_workers=4
        )

    def two_hop_aggregation(self, customer_id: str) -> Dict[str, Any]:
        """
        Calculate 2-hop aggregation features for a customer using Gremlin.

        Args:
            customer_id: Customer ID to analyze

        Returns:
            Dictionary with aggregated features
        """
        # Gremlin query for 2-hop traversal with aggregations
        query = """
        g.V().has('Customer', 'customer_id', customer_id)
          .out('TRANSFERRED')
          .has('status', 'completed')
          .dedup()
          .out('TRANSFERRED')
          .has('status', 'completed')
          .fold()
          .project('num_unique_2hop_receivers', 'num_2hop_transactions',
                   'avg_amount', 'total_amount', 'max_amount', 'min_amount',
                   'avg_risk_score', 'avg_balance')
          .by(__.unfold().dedup().count())
          .by(__.unfold().count())
          .by(__.unfold().values('amount').mean())
          .by(__.unfold().values('amount').sum())
          .by(__.unfold().values('amount').max())
          .by(__.unfold().values('amount').min())
          .by(__.unfold().values('risk_score').mean())
          .by(__.unfold().values('account_balance').mean())
        """

        try:
            result = self.client.submit(query, {'customer_id': customer_id}).all().result(timeout=30)

            if not result or len(result) == 0:
                return {
                    'num_unique_2hop_receivers': 0,
                    'num_2hop_transactions': 0,
                    'avg_2hop_transaction_amount': 0.0,
                    'total_2hop_transaction_amount': 0.0,
                    'max_2hop_transaction_amount': 0.0,
                    'min_2hop_transaction_amount': 0.0,
                    'avg_2hop_receiver_risk_score': 0.0,
                    'avg_2hop_receiver_balance': 0.0
                }

            record = result[0]

            features = {
                'num_unique_2hop_receivers': record.get('num_unique_2hop_receivers', 0),
                'num_2hop_transactions': record.get('num_2hop_transactions', 0),
                'avg_2hop_transaction_amount': float(record.get('avg_amount', 0) or 0),
                'total_2hop_transaction_amount': float(record.get('total_amount', 0) or 0),
                'max_2hop_transaction_amount': float(record.get('max_amount', 0) or 0),
                'min_2hop_transaction_amount': float(record.get('min_amount', 0) or 0),
                'avg_2hop_receiver_risk_score': float(record.get('avg_risk_score', 0) or 0),
                'avg_2hop_receiver_balance': float(record.get('avg_balance', 0) or 0)
            }

            return features

        except Exception as e:
            error_msg = str(e)
            if "timeout" in error_msg.lower():
                print(f"PuppyGraph query timeout - schema may not be loaded")
            elif "no schema" in error_msg.lower() or "not found" in error_msg.lower():
                print(f"PuppyGraph schema not loaded - skipping query")
            else:
                print(f"PuppyGraph query error: {error_msg[:100]}")
            return {
                'num_unique_2hop_receivers': 0,
                'num_2hop_transactions': 0,
                'avg_2hop_transaction_amount': 0.0,
                'total_2hop_transaction_amount': 0.0,
                'max_2hop_transaction_amount': 0.0,
                'min_2hop_transaction_amount': 0.0,
                'avg_2hop_receiver_risk_score': 0.0,
                'avg_2hop_receiver_balance': 0.0
            }

    def batch_two_hop_aggregation(self, customer_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Calculate 2-hop aggregation for multiple customers.

        Args:
            customer_ids: List of customer IDs

        Returns:
            List of feature dictionaries
        """
        results = []
        for customer_id in customer_ids:
            features = self.two_hop_aggregation(customer_id)
            features['customer_id'] = customer_id
            results.append(features)
        return results

    def close(self):
        """Close Gremlin client."""
        if self.client:
            self.client.close()
