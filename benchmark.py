"""
Main benchmark script to compare performance of PuppyGraph, Neo4j, and PostgreSQL.
"""
import time
import random
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Dict, Any
from queries import PostgresQueries, Neo4jQueries, PuppyGraphQueries
from config import DatabaseConfig


class PerformanceBenchmark:
    """Comprehensive performance benchmark for graph databases."""

    def __init__(self):
        """Initialize benchmark."""
        self.config = DatabaseConfig()
        self.results: Dict[str, Any] = {}

    def select_test_customers(self, num_customers: int = 100) -> List[str]:
        """
        Select random customers for testing.

        Args:
            num_customers: Number of customers to test

        Returns:
            List of customer IDs
        """
        print(f"Selecting {num_customers} random customers for testing...")

        # Load customer data
        customers_df = pd.read_csv(f"{self.config.DATA_DIR}/customers.csv")

        # Select random sample
        sample = customers_df.sample(n=min(num_customers, len(customers_df)), random_state=42)
        customer_ids = sample['customer_id'].tolist()

        print(f"Selected {len(customer_ids)} customers")
        return customer_ids

    def benchmark_postgres(self, customer_ids: List[str]) -> Dict[str, Any]:
        """
        Benchmark PostgreSQL performance.

        Args:
            customer_ids: List of customer IDs to test

        Returns:
            Benchmark results dictionary
        """
        print("\n" + "=" * 60)
        print("Benchmarking PostgreSQL")
        print("=" * 60)

        try:
            pg_queries = PostgresQueries()

            # Warm-up query
            print("Running warm-up query...")
            pg_queries.two_hop_aggregation(customer_ids[0])

            # Benchmark single query
            print("Benchmarking single query...")
            start_time = time.time()
            result = pg_queries.two_hop_aggregation(customer_ids[0])
            single_query_time = time.time() - start_time

            # Benchmark batch queries
            print(f"Benchmarking batch queries ({len(customer_ids)} customers)...")
            start_time = time.time()
            results = pg_queries.batch_two_hop_aggregation(customer_ids)
            batch_time = time.time() - start_time
            avg_query_time = batch_time / len(customer_ids)

            pg_queries.close()

            benchmark_results = {
                'database': 'PostgreSQL',
                'single_query_time': single_query_time,
                'batch_total_time': batch_time,
                'batch_size': len(customer_ids),
                'avg_query_time': avg_query_time,
                'queries_per_second': len(customer_ids) / batch_time,
                'sample_result': result,
                'all_results': results
            }

            print(f"  Single query time: {single_query_time:.4f} seconds")
            print(f"  Batch time ({len(customer_ids)} queries): {batch_time:.4f} seconds")
            print(f"  Average query time: {avg_query_time:.4f} seconds")
            print(f"  Queries per second: {benchmark_results['queries_per_second']:.2f}")

            return benchmark_results

        except Exception as e:
            print(f"Error benchmarking PostgreSQL: {e}")
            return {
                'database': 'PostgreSQL',
                'error': str(e),
                'single_query_time': None,
                'batch_total_time': None,
                'batch_size': len(customer_ids),
                'avg_query_time': None,
                'queries_per_second': None
            }

    def benchmark_neo4j(self, customer_ids: List[str]) -> Dict[str, Any]:
        """
        Benchmark Neo4j performance.

        Args:
            customer_ids: List of customer IDs to test

        Returns:
            Benchmark results dictionary
        """
        print("\n" + "=" * 60)
        print("Benchmarking Neo4j")
        print("=" * 60)

        try:
            neo4j_queries = Neo4jQueries()

            # Warm-up query
            print("Running warm-up query...")
            neo4j_queries.two_hop_aggregation(customer_ids[0])

            # Benchmark single query
            print("Benchmarking single query...")
            start_time = time.time()
            result = neo4j_queries.two_hop_aggregation(customer_ids[0])
            single_query_time = time.time() - start_time

            # Benchmark batch queries
            print(f"Benchmarking batch queries ({len(customer_ids)} customers)...")
            start_time = time.time()
            results = neo4j_queries.batch_two_hop_aggregation(customer_ids)
            batch_time = time.time() - start_time
            avg_query_time = batch_time / len(customer_ids)

            neo4j_queries.close()

            benchmark_results = {
                'database': 'Neo4j',
                'single_query_time': single_query_time,
                'batch_total_time': batch_time,
                'batch_size': len(customer_ids),
                'avg_query_time': avg_query_time,
                'queries_per_second': len(customer_ids) / batch_time,
                'sample_result': result,
                'all_results': results
            }

            print(f"  Single query time: {single_query_time:.4f} seconds")
            print(f"  Batch time ({len(customer_ids)} queries): {batch_time:.4f} seconds")
            print(f"  Average query time: {avg_query_time:.4f} seconds")
            print(f"  Queries per second: {benchmark_results['queries_per_second']:.2f}")

            return benchmark_results

        except Exception as e:
            print(f"Error benchmarking Neo4j: {e}")
            return {
                'database': 'Neo4j',
                'error': str(e),
                'single_query_time': None,
                'batch_total_time': None,
                'batch_size': len(customer_ids),
                'avg_query_time': None,
                'queries_per_second': None
            }

    def benchmark_puppygraph(self, customer_ids: List[str]) -> Dict[str, Any]:
        """
        Benchmark PuppyGraph performance.

        Args:
            customer_ids: List of customer IDs to test

        Returns:
            Benchmark results dictionary
        """
        print("\n" + "=" * 60)
        print("Benchmarking PuppyGraph")
        print("=" * 60)

        try:
            pg_queries = PuppyGraphQueries()

            # Warm-up query
            print("Running warm-up query...")
            warmup_result = pg_queries.two_hop_aggregation(customer_ids[0])

            # Check if query returned valid results (not all zeros)
            if warmup_result.get('num_2hop_transactions', 0) == 0:
                print("\n⚠️  WARNING: PuppyGraph schema not loaded!")
                print("PuppyGraph queries are returning empty results.")
                print("Please load the schema via the web UI: http://localhost:8081")
                print("Schema file: puppygraph_schema.json")
                print("\nSkipping PuppyGraph benchmark...\n")

                pg_queries.close()
                return {
                    'database': 'PuppyGraph',
                    'error': 'Schema not loaded - please configure PuppyGraph via web UI',
                    'single_query_time': None,
                    'batch_total_time': None,
                    'batch_size': len(customer_ids),
                    'avg_query_time': None,
                    'queries_per_second': None
                }

            # Benchmark single query
            print("Benchmarking single query...")
            start_time = time.time()
            result = pg_queries.two_hop_aggregation(customer_ids[0])
            single_query_time = time.time() - start_time

            # Benchmark batch queries
            print(f"Benchmarking batch queries ({len(customer_ids)} customers)...")
            start_time = time.time()
            results = pg_queries.batch_two_hop_aggregation(customer_ids)
            batch_time = time.time() - start_time
            avg_query_time = batch_time / len(customer_ids)

            pg_queries.close()

            benchmark_results = {
                'database': 'PuppyGraph',
                'single_query_time': single_query_time,
                'batch_total_time': batch_time,
                'batch_size': len(customer_ids),
                'avg_query_time': avg_query_time,
                'queries_per_second': len(customer_ids) / batch_time,
                'sample_result': result,
                'all_results': results
            }

            print(f"  Single query time: {single_query_time:.4f} seconds")
            print(f"  Batch time ({len(customer_ids)} queries): {batch_time:.4f} seconds")
            print(f"  Average query time: {avg_query_time:.4f} seconds")
            print(f"  Queries per second: {benchmark_results['queries_per_second']:.2f}")

            return benchmark_results

        except Exception as e:
            print(f"\n⚠️  PuppyGraph benchmark failed: {e}")
            print("This is likely because the schema hasn't been loaded.")
            print("PuppyGraph requires manual schema configuration via web UI.")
            print("See puppygraph_setup.py output for instructions.\n")
            return {
                'database': 'PuppyGraph',
                'error': str(e),
                'single_query_time': None,
                'batch_total_time': None,
                'batch_size': len(customer_ids),
                'avg_query_time': None,
                'queries_per_second': None
            }

    def generate_comparison_report(self, postgres_results: Dict, neo4j_results: Dict,
                                   puppygraph_results: Dict) -> None:
        """
        Generate comprehensive comparison report.

        Args:
            postgres_results: PostgreSQL benchmark results
            neo4j_results: Neo4j benchmark results
            puppygraph_results: PuppyGraph benchmark results
        """
        print("\n" + "=" * 80)
        print("PERFORMANCE COMPARISON REPORT")
        print("=" * 80)

        # Check if any database failed
        failed_dbs = []
        if 'error' in postgres_results:
            failed_dbs.append('PostgreSQL')
        if 'error' in neo4j_results:
            failed_dbs.append('Neo4j')
        if 'error' in puppygraph_results:
            failed_dbs.append('PuppyGraph')

        if failed_dbs:
            print("\n⚠️  Note: The following databases were unavailable:")
            for db in failed_dbs:
                print(f"   - {db}")
            if 'PuppyGraph' in failed_dbs:
                print("\n   PuppyGraph requires manual schema loading via web UI.")
                print("   Visit http://localhost:8081 to configure the schema.")
            print()

        # Create summary table
        summary_data = []
        for result in [postgres_results, neo4j_results, puppygraph_results]:
            if 'error' not in result:
                summary_data.append({
                    'Database': result['database'],
                    'Single Query Time (s)': f"{result['single_query_time']:.4f}",
                    'Avg Query Time (s)': f"{result['avg_query_time']:.4f}",
                    'Queries/Second': f"{result['queries_per_second']:.2f}",
                    'Total Batch Time (s)': f"{result['batch_total_time']:.2f}"
                })
            else:
                summary_data.append({
                    'Database': result['database'],
                    'Single Query Time (s)': 'ERROR',
                    'Avg Query Time (s)': 'ERROR',
                    'Queries/Second': 'ERROR',
                    'Total Batch Time (s)': 'ERROR'
                })

        summary_df = pd.DataFrame(summary_data)
        print("\n" + summary_df.to_string(index=False))

        # Calculate speedup factors (only if we have valid results)
        successful_dbs = [r for r in [postgres_results, neo4j_results, puppygraph_results] if 'error' not in r]

        if len(successful_dbs) >= 2:
            print("\n" + "=" * 80)
            print("SPEEDUP ANALYSIS (relative to PostgreSQL)")
            print("=" * 80)

            if 'error' not in postgres_results:
                postgres_time = postgres_results['avg_query_time']

                if 'error' not in neo4j_results:
                    neo4j_speedup = postgres_time / neo4j_results['avg_query_time']
                    print(f"Neo4j speedup: {neo4j_speedup:.2f}x")

                if 'error' not in puppygraph_results:
                    puppygraph_speedup = postgres_time / puppygraph_results['avg_query_time']
                    print(f"PuppyGraph speedup: {puppygraph_speedup:.2f}x")
            else:
                print("Cannot calculate speedup - PostgreSQL benchmark unavailable")
        else:
            print("\n⚠️  Insufficient data for speedup analysis (need at least 2 databases)")

        # Save results
        self.save_results(postgres_results, neo4j_results, puppygraph_results)

        # Generate visualizations
        self.generate_visualizations(summary_data)

        print("\n" + "=" * 80)
        print("Report saved to: results/")
        print("=" * 80)

    def save_results(self, postgres_results: Dict, neo4j_results: Dict,
                    puppygraph_results: Dict) -> None:
        """
        Save benchmark results to CSV files.

        Args:
            postgres_results: PostgreSQL results
            neo4j_results: Neo4j results
            puppygraph_results: PuppyGraph results
        """
        import os

        results_dir = "results"
        os.makedirs(results_dir, exist_ok=True)

        # Save summary
        summary_data = []
        for result in [postgres_results, neo4j_results, puppygraph_results]:
            if 'error' not in result:
                summary_data.append({
                    'database': result['database'],
                    'single_query_time': result['single_query_time'],
                    'avg_query_time': result['avg_query_time'],
                    'queries_per_second': result['queries_per_second'],
                    'batch_total_time': result['batch_total_time'],
                    'batch_size': result['batch_size']
                })

        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(f"{results_dir}/benchmark_summary.csv", index=False)

        # Save detailed results for each database
        for result in [postgres_results, neo4j_results, puppygraph_results]:
            if 'error' not in result and 'all_results' in result:
                db_name = result['database'].lower().replace(' ', '_')
                results_df = pd.DataFrame(result['all_results'])
                results_df.to_csv(f"{results_dir}/{db_name}_detailed_results.csv", index=False)

        print(f"Results saved to {results_dir}/")

    def generate_visualizations(self, summary_data: List[Dict]) -> None:
        """
        Generate performance visualization charts.

        Args:
            summary_data: Summary data for visualization
        """
        import os

        results_dir = "results"
        os.makedirs(results_dir, exist_ok=True)

        # Set style
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (12, 8)

        # Extract data for successful benchmarks
        valid_data = [d for d in summary_data if d['Avg Query Time (s)'] != 'ERROR']

        if not valid_data:
            print("No valid data for visualization")
            return

        databases = [d['Database'] for d in valid_data]
        avg_times = [float(d['Avg Query Time (s)']) for d in valid_data]
        qps = [float(d['Queries/Second']) for d in valid_data]

        # Create subplots
        fig, axes = plt.subplots(1, 2, figsize=(15, 6))

        # Plot 1: Average Query Time
        axes[0].bar(databases, avg_times, color=['#3498db', '#e74c3c', '#2ecc71'])
        axes[0].set_ylabel('Average Query Time (seconds)', fontsize=12)
        axes[0].set_title('Average Query Time Comparison', fontsize=14, fontweight='bold')
        axes[0].grid(axis='y', alpha=0.3)

        # Add value labels on bars
        for i, v in enumerate(avg_times):
            axes[0].text(i, v, f'{v:.4f}s', ha='center', va='bottom', fontweight='bold')

        # Plot 2: Queries per Second
        axes[1].bar(databases, qps, color=['#3498db', '#e74c3c', '#2ecc71'])
        axes[1].set_ylabel('Queries per Second', fontsize=12)
        axes[1].set_title('Throughput Comparison', fontsize=14, fontweight='bold')
        axes[1].grid(axis='y', alpha=0.3)

        # Add value labels on bars
        for i, v in enumerate(qps):
            axes[1].text(i, v, f'{v:.2f}', ha='center', va='bottom', fontweight='bold')

        plt.tight_layout()
        plt.savefig(f'{results_dir}/performance_comparison.png', dpi=300, bbox_inches='tight')
        print(f"Visualization saved to {results_dir}/performance_comparison.png")
        plt.close()

    def run_full_benchmark(self, num_test_customers: int = 100) -> None:
        """
        Run complete benchmark for all databases.

        Args:
            num_test_customers: Number of customers to test
        """
        print("=" * 80)
        print("GRAPH DATABASE PERFORMANCE BENCHMARK")
        print("=" * 80)
        print(f"Test Configuration:")
        print(f"  - Number of customers: {self.config.NUM_CUSTOMERS}")
        print(f"  - Number of transactions: {self.config.NUM_TRANSACTIONS}")
        print(f"  - Number of card transactions: {self.config.NUM_CARD_TRANSACTIONS}")
        print(f"  - Test sample size: {num_test_customers}")
        print("=" * 80)

        # Select test customers
        customer_ids = self.select_test_customers(num_test_customers)

        # Run benchmarks
        postgres_results = self.benchmark_postgres(customer_ids)
        neo4j_results = self.benchmark_neo4j(customer_ids)
        puppygraph_results = self.benchmark_puppygraph(customer_ids)

        # Generate comparison report
        self.generate_comparison_report(postgres_results, neo4j_results, puppygraph_results)


def main():
    """Main function to run benchmark."""
    benchmark = PerformanceBenchmark()

    # Run benchmark with 100 test customers (can be adjusted)
    benchmark.run_full_benchmark(num_test_customers=100)


if __name__ == "__main__":
    main()
