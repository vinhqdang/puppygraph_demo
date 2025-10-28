"""
Main runner script to execute complete benchmark pipeline.
"""
import sys
import time
from data_generator import main as generate_data
from postgres_setup import main as setup_postgres
from neo4j_setup import main as setup_neo4j
from puppygraph_setup import main as setup_puppygraph
from benchmark import main as run_benchmark


class BenchmarkPipeline:
    """Complete benchmark pipeline orchestrator."""

    def __init__(self):
        """Initialize pipeline."""
        self.steps = [
            ("Generate Banking Data", generate_data),
            ("Setup PostgreSQL", setup_postgres),
            ("Setup Neo4j", setup_neo4j),
            ("Setup PuppyGraph", setup_puppygraph),
            ("Run Performance Benchmark", run_benchmark)
        ]

    def run(self, skip_data_generation: bool = False, skip_setup: bool = False) -> None:
        """
        Run complete benchmark pipeline.

        Args:
            skip_data_generation: Skip data generation step
            skip_setup: Skip database setup steps
        """
        print("\n" + "=" * 80)
        print(" " * 20 + "GRAPH DATABASE BENCHMARK PIPELINE")
        print("=" * 80)

        total_start_time = time.time()

        for i, (step_name, step_func) in enumerate(self.steps, 1):
            # Skip steps if requested
            if skip_data_generation and step_name == "Generate Banking Data":
                print(f"\n[Step {i}/{len(self.steps)}] Skipping: {step_name}")
                continue

            if skip_setup and step_name in ["Setup PostgreSQL", "Setup Neo4j", "Setup PuppyGraph"]:
                print(f"\n[Step {i}/{len(self.steps)}] Skipping: {step_name}")
                continue

            print(f"\n[Step {i}/{len(self.steps)}] Running: {step_name}")
            print("-" * 80)

            try:
                step_start_time = time.time()
                step_func()
                step_time = time.time() - step_start_time
                print(f"\n[Step {i}/{len(self.steps)}] Completed in {step_time:.2f} seconds")
            except Exception as e:
                print(f"\n[Step {i}/{len(self.steps)}] ERROR: {e}")
                print("Pipeline stopped due to error.")
                sys.exit(1)

        total_time = time.time() - total_start_time

        print("\n" + "=" * 80)
        print(f" " * 25 + "PIPELINE COMPLETED")
        print("=" * 80)
        print(f"Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        print("=" * 80)


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run complete graph database benchmark pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_all.py                          # Run complete pipeline
  python run_all.py --skip-data              # Skip data generation
  python run_all.py --skip-setup             # Skip database setup
  python run_all.py --skip-data --skip-setup # Run only benchmark
        """
    )

    parser.add_argument(
        '--skip-data',
        action='store_true',
        help='Skip data generation step (use existing data)'
    )

    parser.add_argument(
        '--skip-setup',
        action='store_true',
        help='Skip database setup steps (use existing database configurations)'
    )

    args = parser.parse_args()

    pipeline = BenchmarkPipeline()
    pipeline.run(
        skip_data_generation=args.skip_data,
        skip_setup=args.skip_setup
    )


if __name__ == "__main__":
    main()
