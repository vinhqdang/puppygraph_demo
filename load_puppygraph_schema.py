"""
Script to load schema into PuppyGraph via REST API.
"""
import json
import requests
import base64
import sys


def load_schema_to_puppygraph(schema_file="puppygraph_schema.json",
                                host="localhost",
                                port="8081",
                                password="puppygraph123"):
    """Load schema into PuppyGraph using REST API."""

    print(f"Loading schema from {schema_file} into PuppyGraph...")

    # Read schema file
    try:
        with open(schema_file, 'r') as f:
            schema = json.load(f)
        print(f"✓ Schema file loaded successfully")
    except FileNotFoundError:
        print(f"✗ Error: Schema file '{schema_file}' not found")
        return False
    except json.JSONDecodeError as e:
        print(f"✗ Error: Invalid JSON in schema file: {e}")
        return False

    # Prepare API request
    base_url = f"http://{host}:{port}"

    # Try different API endpoints and auth methods
    endpoints = [
        "/api/v1/schema",
        "/schema",
        "/api/schema",
    ]

    # Encode credentials for Basic auth
    auth_string = f"puppygraph:{password}"
    auth_bytes = auth_string.encode('ascii')
    base64_bytes = base64.b64encode(auth_bytes)
    base64_auth = base64_bytes.decode('ascii')

    headers_variants = [
        {
            'Content-Type': 'application/json',
            'Authorization': f'Basic {base64_auth}'
        },
        {
            'Content-Type': 'application/json',
            'X-API-Key': password
        },
        {
            'Content-Type': 'application/json',
        }
    ]

    print(f"\nAttempting to load schema into PuppyGraph at {base_url}...")

    for endpoint in endpoints:
        for headers in headers_variants:
            url = f"{base_url}{endpoint}"
            try:
                print(f"  Trying {endpoint} with {list(headers.keys())}...", end=" ")
                response = requests.post(url, json=schema, headers=headers, timeout=10)

                if response.status_code in [200, 201]:
                    print(f"✓ SUCCESS")
                    print(f"\n{'='*60}")
                    print(f"Schema loaded successfully into PuppyGraph!")
                    print(f"{'='*60}")
                    return True
                elif response.status_code == 404:
                    print(f"✗ Not found")
                elif response.status_code == 401:
                    print(f"✗ Unauthorized")
                elif response.status_code == 400:
                    print(f"✗ Bad Request")
                    print(f"    Full error: {response.text}")
                else:
                    print(f"✗ Status {response.status_code}: {response.text}")
            except requests.exceptions.RequestException as e:
                print(f"✗ Error: {e}")

    print(f"\n{'='*60}")
    print(f"Could not automatically load schema into PuppyGraph")
    print(f"{'='*60}")
    print(f"Please manually load the schema using one of these methods:")
    print(f"1. Web UI: http://{host}:{port}")
    print(f"   - Navigate to Schema/Configuration section")
    print(f"   - Upload or paste the schema JSON")
    print(f"")
    print(f"2. If PuppyGraph provides a CLI tool, use:")
    print(f"   puppygraph-cli load-schema {schema_file}")
    print(f"{'='*60}")

    return False


if __name__ == "__main__":
    schema_file = sys.argv[1] if len(sys.argv) > 1 else "puppygraph_schema.json"
    success = load_schema_to_puppygraph(schema_file)
    sys.exit(0 if success else 1)
