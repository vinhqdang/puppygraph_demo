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
        # Use Docker container path since PuppyGraph runs in Docker
        # The data directory is mounted as /data in the container
        data_dir = "/data"

        # Simplified schema format for PuppyGraph
        # PuppyGraph will auto-detect column names from CSV headers
        schema = {
            "vertices": [
                {
                    "label": "Customer",
                    "file": f"{data_dir}/customers.csv",
                    "id": "customer_id"
                }
            ],
            "edges": [
                {
                    "label": "TRANSFERRED",
                    "file": f"{data_dir}/transactions.csv",
                    "from": "Customer",
                    "from_id": "from_customer_id",
                    "to": "Customer",
                    "to_id": "to_customer_id"
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

    def load_schema_via_file(self) -> bool:
        """
        Copy schema file into PuppyGraph container for loading.

        Returns:
            True if schema file copied successfully
        """
        print("Preparing schema for PuppyGraph...")
        try:
            schema_path = os.path.join(os.path.dirname(__file__), "puppygraph_schema.json")

            # Copy schema file into PuppyGraph container
            import subprocess
            result = subprocess.run(
                ["docker", "cp", schema_path, "graph_benchmark_puppygraph:/tmp/schema.json"],
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                print(f"Schema file copied to PuppyGraph container")
                print(f"\nTo load the schema, you have two options:")
                print(f"1. Web UI: Visit http://localhost:8081 and upload /tmp/schema.json")
                print(f"2. Command line:")
                print(f"   docker exec graph_benchmark_puppygraph puppygraph-cli import-schema /tmp/schema.json")
                return True
            else:
                print(f"Failed to copy schema file: {result.stderr}")
                return False

        except Exception as e:
            print(f"Error preparing schema: {e}")
            return False

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

        # Provide instructions for loading schema
        print("\n" + "=" * 60)
        print("PuppyGraph Schema Setup Instructions")
        print("=" * 60)
        print(f"Schema file generated at: {schema_path}")
        print(f"\nPuppyGraph is running at: http://localhost:8081")
        print(f"")
        print(f"⚠️  MANUAL STEP REQUIRED:")
        print(f"PuppyGraph schema must be loaded through the web UI.")
        print(f"")
        print(f"Steps to load schema:")
        print(f"1. Open http://localhost:8081 in your browser")
        print(f"2. Login with password: puppygraph123")
        print(f"3. Navigate to Schema/Configuration section")
        print(f"4. Upload the generated schema file:")
        print(f"   {schema_path}")
        print(f"")
        print(f"📝 Note: The benchmark will proceed without PuppyGraph")
        print(f"   if schema is not loaded. PostgreSQL and Neo4j will")
        print(f"   still be benchmarked successfully.")
        print("=" * 60)

        elapsed_time = time.time() - start_time

        print("\n" + "=" * 60)
        print(f"PuppyGraph setup completed in {elapsed_time:.2f} seconds")
        print("=" * 60)

    except Exception as e:
        print(f"Error during PuppyGraph setup: {e}")
        print("Note: PuppyGraph may not be running. The schema file has been created.")


if __name__ == "__main__":
    main()
