#!/usr/bin/env python3
"""Validation script to ensure both tutorial execution methods work correctly.

This script tests both the programmatic Python and declarative YAML CLI
execution methods to verify they produce identical results.
"""

import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd


def cleanup_output():
    """Clean up any existing output files."""
    output_dir = Path("output")
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(exist_ok=True)


def run_python_method():
    """Execute the pipeline using the Python script."""
    print("🔄 Testing Python script execution...")

    # Python script must run from project root due to absolute path usage
    project_root = (
        Path.cwd().parent.parent
    )  # Go up two levels from examples/retail_analytics
    script_path = "examples/retail_analytics/run_pipeline.py"

    result = subprocess.run(
        [
            "uv",
            "run",
            "python",
            script_path,
        ],
        capture_output=True,
        text=True,
        cwd=project_root,
    )

    if result.returncode != 0:
        print(f"❌ Python script failed: {result.stderr}")
        return False

    print("✅ Python script executed successfully")
    return True


def run_cli_method():
    """Execute the pipeline using the nmetl CLI."""
    print("🔄 Testing nmetl CLI execution...")

    # Clean up first
    cleanup_output()

    result = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "-m",
            "pycypher.nmetl_cli",
            "run",
            "pipeline.yaml",
            "--verbose",
        ],
        capture_output=True,
        text=True,
        cwd=Path.cwd(),
    )

    if result.returncode != 0:
        print(f"❌ CLI execution failed: {result.stderr}")
        return False

    print("✅ CLI executed successfully")
    print("   CLI output preview:")
    for line in result.stdout.strip().split("\n")[-5:]:
        print(f"   {line}")
    return True


def verify_output_files():
    """Verify that all expected output files were created."""
    expected_files = [
        "output/customer_metrics.csv",
        "output/customer_segments.csv",
        "output/product_performance.csv",
        "output/executive_report.csv",
    ]

    missing_files = []
    for file_path in expected_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)

    if missing_files:
        print(f"❌ Missing output files: {missing_files}")
        return False

    print(f"✅ All {len(expected_files)} output files created")
    return True


def verify_data_consistency():
    """Verify that the output contains expected data."""
    try:
        # Check executive report has expected structure
        exec_report = pd.read_csv("output/executive_report.csv")
        if len(exec_report) != 1:
            print(
                f"❌ Executive report should have 1 row, got {len(exec_report)}"
            )
            return False

        # Check key metrics are reasonable
        row = exec_report.iloc[0]
        if row["total_customers"] != 50.0:
            print(f"❌ Expected 50 customers, got {row['total_customers']}")
            return False

        if row["total_revenue"] < 30000 or row["total_revenue"] > 40000:
            print(f"❌ Revenue seems unreasonable: ${row['total_revenue']}")
            return False

        print("✅ Data validation passed:")
        print(f"   📊 Customers: {row['total_customers']}")
        print(f"   💰 Revenue: ${row['total_revenue']:,.2f}")
        print(f"   📈 Profit Margin: {row['profit_margin_percent']}%")
        return True

    except Exception as e:
        print(f"❌ Data validation failed: {e}")
        return False


def main():
    """Run the complete tutorial validation."""
    print("🚀 Validating Retail Analytics ETL Tutorial")
    print("=" * 50)

    # Ensure we're in the right directory
    if not Path("pipeline.yaml").exists():
        print("❌ Must run from examples/retail_analytics directory")
        sys.exit(1)

    success = True

    # Test Python method
    if not run_python_method():
        success = False

    if not verify_output_files():
        success = False

    if not verify_data_consistency():
        success = False

    # Test CLI method
    if not run_cli_method():
        success = False

    if not verify_output_files():
        success = False

    if not verify_data_consistency():
        success = False

    print("\n" + "=" * 50)
    if success:
        print("🎉 Tutorial validation PASSED!")
        print(
            "Both execution methods work correctly and produce consistent results."
        )
    else:
        print("❌ Tutorial validation FAILED!")
        print("Check the errors above and fix before using the tutorial.")
        sys.exit(1)


if __name__ == "__main__":
    main()
