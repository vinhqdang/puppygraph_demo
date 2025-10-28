"""
PuppyGraph setup and schema configuration.
PuppyGraph reads data directly from CSV files without loading into its own storage.
"""
import os
import json
import requests
import time
from gremlin_python.driver import client, serializer
from config import DatabaseConfig


class PuppyGraphSetup:
    """Setup PuppyGraph schema and verify connection."""

    def __init__(self):
        """Initialize PuppyGraph setup."""
        self.config = DatabaseConfig()
        self.base_url = f"http://{self.config.PUPPYGRAPH_HOST}:{self.config.PUPPYGRAPH_PORT}"
        self.gremlin_url = f"ws://{self.config.PUPPYGRAPH_HOST}:{self.config.PUPPYGRAPH_GREMLIN_PORT}/gremlin"

    def verify_connection(self) -> bool:
        """
        Verify PuppyGraph is running.

        Returns:
            True if connected successfully
        """
        print("Verifying PuppyGraph connection...")
        try:
            # Check if HTTP endpoint is available (sufficient for verification)
            response = requests.get(f"{self.base_url}", timeout=5)
            if response.status_code == 200:
                print("PuppyGraph HTTP endpoint is accessible")
                print("PuppyGraph connection verified")
                return True
            else:
                print(f"PuppyGraph HTTP endpoint returned status {response.status_code}")
                return False

        except requests.exceptions.Timeout:
            print("Failed to connect to PuppyGraph: Connection timeout")
            print("Please ensure PuppyGraph is running on localhost with default ports")
            return False
        except Exception as e:
            print(f"Failed to connect to PuppyGraph: {e}")
            print("Please ensure PuppyGraph is running on localhost with default ports")
            return False

    def create_schema_config(self) -> dict:
        """
        Create PuppyGraph schema configuration for banking data.

        Returns:
            Schema configuration dictionary
        """
        data_dir = os.path.abspath(self.config.DATA_DIR)

        schema = {
            "vertices": [
                {
                    "label": "Customer",
                    "source": {
                        "type": "csv",
                        "path": f"{data_dir}/customers.csv",
                        "delimiter": ","
                    },
                    "id": "customer_id",
                    "attributes": [
                        {"name": "customer_id", "type": "string"},
                        {"name": "name", "type": "string"},
                        {"name": "email", "type": "string"},
                        {"name": "account_balance", "type": "double"},
                        {"name": "risk_score", "type": "double"},
                        {"name": "account_type", "type": "string"},
                        {"name": "registration_date", "type": "string"}
                    ]
                }
            ],
            "edges": [
                {
                    "label": "TRANSFERRED",
                    "source": {
                        "type": "csv",
                        "path": f"{data_dir}/transactions.csv",
                        "delimiter": ","
                    },
                    "from": {
                        "vertex_label": "Customer",
                        "id_field": "from_customer_id"
                    },
                    "to": {
                        "vertex_label": "Customer",
                        "id_field": "to_customer_id"
                    },
                    "attributes": [
                        {"name": "transaction_id", "type": "string"},
                        {"name": "amount", "type": "double"},
                        {"name": "transaction_date", "type": "string"},
                        {"name": "transaction_type", "type": "string"},
                        {"name": "status", "type": "string"}
                    ]
                }
            ]
        }

        return schema

    def save_schema_config(self) -> str:
        """
        Save schema configuration to JSON file.

        Returns:
            Path to schema file
        """
        schema = self.create_schema_config()
        schema_path = os.path.join(os.path.dirname(__file__), "puppygraph_schema.json")

        with open(schema_path, 'w') as f:
            json.dump(schema, f, indent=2)

        print(f"Schema configuration saved to: {schema_path}")
        return schema_path

    def test_query(self) -> None:
        """Test basic queries on PuppyGraph."""
        print("\nTesting PuppyGraph queries...")

        try:
            gremlin_client = client.Client(
                self.gremlin_url,
                'g',
                message_serializer=serializer.GraphSONSerializersV2d0(),
                pool_size=1,
                max_workers=1
            )

            # Test 1: Count vertices
            print("  Test 1: Counting customers...")
            result = gremlin_client.submit("g.V().hasLabel('Customer').count()").all().result(timeout=30)
            print(f"    Found {result[0]} customers")

            # Test 2: Count edges
            print("  Test 2: Counting transactions...")
            result = gremlin_client.submit("g.E().hasLabel('TRANSFERRED').count()").all().result(timeout=30)
            print(f"    Found {result[0]} transactions")

            # Test 3: Sample customer
            print("  Test 3: Sampling a customer...")
            result = gremlin_client.submit("g.V().hasLabel('Customer').limit(1).valueMap()").all().result(timeout=10)
            if result:
                print(f"    Sample customer: {result[0]}")

            gremlin_client.close()
            print("PuppyGraph queries test completed successfully")

        except Exception as e:
            print(f"Error during query test: {e}")
            raise


def main():
    """Main function to setup PuppyGraph."""
    print("=" * 60)
    print("PuppyGraph Setup")
    print("=" * 60)

    setup = PuppyGraphSetup()

    try:
        start_time = time.time()

        # Verify connection
        if not setup.verify_connection():
            print("\nIMPORTANT: Please ensure PuppyGraph is running before proceeding.")
            print("You can start PuppyGraph and configure it with the schema file.")
            print("\nGenerating schema configuration file...")

        # Create and save schema configuration
        schema_path = setup.save_schema_config()

        print("\n" + "=" * 60)
        print("PuppyGraph Setup Instructions:")
        print("=" * 60)
        print("1. Ensure PuppyGraph is running on localhost")
        print("2. Use the generated schema file to configure PuppyGraph:")
        print(f"   {schema_path}")
        print("3. Load the schema into PuppyGraph using the web UI or API")
        print("=" * 60)

        # Try to test queries if connected (skip if schema not loaded)
        if setup.verify_connection():
            print("\nNote: Query tests skipped. Load the schema first using PuppyGraph UI/API.")
            print("After loading the schema, you can test queries manually.")

        elapsed_time = time.time() - start_time

        print("\n" + "=" * 60)
        print(f"PuppyGraph setup completed in {elapsed_time:.2f} seconds")
        print("=" * 60)

    except Exception as e:
        print(f"Error during PuppyGraph setup: {e}")
        print("Note: PuppyGraph may not be running. The schema file has been created.")


if __name__ == "__main__":
    main()
