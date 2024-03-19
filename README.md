
# Subscription Data Processing Script

## Overview

This PySpark script processes subscription data for various countries (Sweden, Denmark, Finland, Norway) stored in Azure Data Lake Storage (ADLS). It consolidates and transforms subscription prices, details, customer details, and account information from the Silver layer to the Gold layer, applying specific transformations and schema adjustments to prepare the data for further analytics and reporting purposes.

## Prerequisites

- Apache Spark environment with PySpark
- Access to Azure Data Lake Storage Gen2
- Azure Synapse Studio or a similar environment to execute PySpark scripts

## Dependencies

- PySpark SQL module
- PySpark SQL functions

## Configuration

Set the `base_path` variable to the root path of your data in ADLS. Ensure that the Silver datasets for subscription prices, details, and customer information are stored in the specified structure under the `base_path`.

```python
base_path = 'abfss://[your_storage_account].dfs.core.windows.net/'
```

## Usage

1. **Data Loading**: The script starts by loading various datasets from the Silver layer (e.g., subscription prices, details, customer details) for each country.
2. **Data Transformation**: It applies transformations such as renaming columns, creating new columns, and converting data types.
3. **Data Consolidation**: Different datasets are consolidated (union operations) based on their country and category (e.g., subscription prices, subscription details).
4. **Data Joining**: The script performs several join operations to combine related datasets based on specific keys (e.g., subscription IDs, customer IDs).
5. **Writing to Gold Layer**: The transformed and consolidated datasets are written to the Gold layer in Delta format, ready for analytics and reporting.
6. **Schema Management**: Schemas are displayed and managed throughout the process to ensure data consistency and integrity.

## Key Operations

- **Union Operations**: Combine similar datasets from different countries.
- **Column Renaming and Creation**: Adjust schema to fit target models.
- **Join Operations**: Merge related datasets on specific keys.
- **Data Filtering**: Remove or filter out unnecessary or null data.
- **Schema Adjustments**: Ensure the datasets align with target Gold layer schemas.
- **Data Writing**: Save the transformed datasets to the Gold layer in Delta format.

## Running the Script

Execute the script in your PySpark environment. Monitor the output for any errors or warnings related to data loading, transformation, or writing operations.

## Maintenance and Support

Regularly update the `base_path` and other configurations as necessary to adapt to changes in the storage structure or schema. For support, consult the PySpark documentation or contact the data engineering team responsible for the Azure Synapse environment.
