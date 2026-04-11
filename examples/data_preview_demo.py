#!/usr/bin/env python3
"""Demo script for data preview functionality in TUI.

This script demonstrates the data preview capability by creating sample
data files and showing how the preview dialog works with different file types.
"""

import json
import tempfile
from pathlib import Path

import pandas as pd
from pycypher_tui.widgets.data_preview import DataPreviewDialog


def create_sample_data():
    """Create sample CSV and JSON files for demonstration."""
    files = {}

    # Create CSV file with customer data
    csv_data = pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "name": ["Alice Smith", "Bob Johnson", "Charlie Brown", "Diana Prince", "Eve Wilson"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "diana@example.com", "eve@example.com"],
        "age": [25, 30, 35, 28, 32],
        "city": ["New York", "London", "Paris", "Tokyo", "Sydney"]
    })

    csv_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    csv_data.to_csv(csv_file.name, index=False)
    csv_file.close()
    files["customers.csv"] = Path(csv_file.name)

    # Create JSON file with order data
    json_data = [
        {"order_id": 1, "customer_id": 1, "product": "Widget A", "quantity": 2, "total": 39.98},
        {"order_id": 2, "customer_id": 2, "product": "Widget B", "quantity": 1, "total": 29.99},
        {"order_id": 3, "customer_id": 3, "product": "Book X", "quantity": 3, "total": 44.97},
        {"order_id": 4, "customer_id": 1, "product": "Book Y", "quantity": 1, "total": 19.99},
        {"order_id": 5, "customer_id": 4, "product": "Widget C", "quantity": 2, "total": 59.98}
    ]

    json_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(json_data, json_file, indent=2)
    json_file.close()
    files["orders.json"] = Path(json_file.name)

    return files


def demo_data_preview():
    """Demonstrate data preview functionality."""
    print("Creating sample data files...")
    files = create_sample_data()

    print(f"Created files:")
    for name, path in files.items():
        print(f"  {name}: {path}")

    print("\nData preview dialog can be used with these files:")
    print("- CSV file with customer data (5 rows, 5 columns)")
    print("- JSON file with order data (5 records)")

    # Test dialog creation (without running the TUI)
    csv_dialog = DataPreviewDialog(
        source_uri=str(files["customers.csv"]),
        source_id="customers"
    )
    print(f"\nCSV dialog created: {csv_dialog.dialog_title}")

    json_dialog = DataPreviewDialog(
        source_uri=str(files["orders.json"]),
        source_id="orders"
    )
    print(f"JSON dialog created: {json_dialog.dialog_title}")

    print("\nIn the TUI:")
    print("1. Navigate to Data Sources screen")
    print("2. Add these files as data sources")
    print("3. Press 'p' to preview any source")
    print("4. Use Tab to switch between Sample Data, Schema, and Statistics tabs")
    print("5. Press Escape to close the preview dialog")

    # Clean up
    print("\nCleaning up temporary files...")
    for path in files.values():
        path.unlink()

    print("Demo complete!")


if __name__ == "__main__":
    demo_data_preview()