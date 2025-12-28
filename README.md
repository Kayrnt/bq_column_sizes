# BigQuery Column Sizes Analyzer

[![Jitpack](https://jitpack.io/v/Kayrnt/bq_column_sizes.svg)](https://jitpack.io/#Kayrnt/bq_column_sizes)

A specialized tool designed to analyze and monitor the storage size of individual columns in BigQuery tables. By leveraging BigQuery's dry-run capabilities, it provides accurate estimates of data volume per column and partition without incurring query costs.

## Key Features

- **Cost-Free Analysis**: Uses BigQuery dry runs to calculate column sizes, ensuring no processing costs are incurred during analysis.
- **Granular Monitoring**: Tracks column sizes at the partition level, allowing for detailed observation of data growth over time.
- **Flexible Scoping**: Analyze entire projects, specific datasets, or individual tables.
- **Multiple Output Formats**: Export results to local CSV files or directly into a BigQuery table for further analysis.
- **Execution Project Support**: Separate the project being analyzed from the project used for query execution (useful for permission or billing management).

## How It Works

The tool iterates through the specified GCP resources (projects, datasets, and tables). For each table, it identifies the partitions and executes a `SELECT` dry run for every column. The resulting "total bytes processed" statistic is then recorded as the column's size for that specific partition.

## Prerequisites

- **GCP Credentials**: Ensure you have active Google Cloud credentials. The tool uses Application Default Credentials (ADC).
  ```bash
  gcloud auth application-default login
  ```
- **Permissions**: The user/service account must have:
  - `bigquery.jobs.create` on the execution project.
  - `bigquery.tables.get` and `bigquery.tables.list` on the target project/datasets.
  - `bigquery.tables.getData` (or metadata access) to perform dry runs.

## Installation

### Quick Start (via Coursier)

The easiest way to run the tool is using [Coursier](https://get-coursier.io/):

1. Fetch the dependency from Jitpack:
   ```bash
   coursier fetch -r jitpack com.github.Kayrnt:bq_column_sizes:master-SNAPSHOT
   ```
2. Launch the tool:
   ```bash
   coursier launch -r jitpack com.github.Kayrnt:bq_column_sizes:master-SNAPSHOT -- --help
   ```

### Building from Source

1. Clone the repository:
   ```bash
   git clone https://github.com/Kayrnt/bq_column_sizes.git
   cd bq_column_sizes
   ```
2. Build and run using SBT:
   ```bash
   sbt "run --help"
   ```

## Usage & Configuration

### Command Line Options

| Option                     | Short   | Description                                                | Default        |
| :------------------------- | :------ | :--------------------------------------------------------- | :------------- |
| `--project-id`             | `-p`    | **(Required)** GCP project ID to analyze.                  | -              |
| `--execution-project-id`   | `-e`    | GCP project ID used for query execution.                   | Same as `-p`   |
| `--dataset`                | `-d`    | Comma-separated list of datasets to analyze.               | All datasets   |
| `--table`                  | `-t`    | Comma-separated list of tables to analyze.                 | All tables     |
| `--output-writer`          | `--ow`  | Output format: `csv` or `bq`.                              | `csv`          |
| `--output-file-path`       | `-o`    | Path for the CSV output file.                              | `size.csv`     |
| `--max-concurrent-queries` | `--mcq` | Maximum number of concurrent BigQuery requests.            | `4`            |
| `--partition`              | `--pt`  | Specific partition to analyze (e.g., `20210101`).          | All partitions |
| `--job-frequency`          | `--jf`  | Partitioning frequency: `year`, `month`, `day`, `hour`.    | `day`          |
| `--offset`                 | -       | Partition offset as duration (e.g., `1d`, `1hour`).        | -              |
| `--write-disposition`      | -       | BQ write disposition: `write_empty`, `truncate`, `append`. | `truncate`     |

### BigQuery Output Options

When using `--ow bq`, the following options are available:

| Option             | Short  | Description                              |
| :----------------- | :----- | :--------------------------------------- |
| `--output-project` | `--op` | Target GCP project for the output table. |
| `--output-dataset` | `--od` | Target dataset for the output table.     |
| `--output-table`   | `--ot` | Target table name for the output.        |

## Examples

### Analyze a Public Dataset and Save to CSV

```bash
bq-column-sizes -p bigquery-public-data -d crypto_bitcoin -t blocks --pt 20210501 -o bitcoin_sizes.csv
```

### Analyze and Export to a BigQuery Table

```bash
bq-column-sizes \
  -p my-source-project \
  -d my_dataset \
  --ow bq \
  --op my-billing-project \
  --od monitoring_dataset \
  --ot column_size_history
```

### Using a Dedicated Execution Project

If you have read access to a project but want to use a different project for query execution (billing):

```bash
bq-column-sizes -p target-project-id -e billing-project-id -d production_dataset
```

## Output Schema

The tool generates data with the following columns:

| Column            | Type      | Description                                         |
| :---------------- | :-------- | :-------------------------------------------------- |
| `job_partition`   | TIMESTAMP | The timestamp of the analysis run.                  |
| `project_id`      | STRING    | The GCP project ID of the analyzed table.           |
| `dataset`         | STRING    | The dataset name.                                   |
| `table`           | STRING    | The table name.                                     |
| `field`           | STRING    | The specific column/field name.                     |
| `table_partition` | TIMESTAMP | The specific partition of the table being analyzed. |
| `size_in_bytes`   | INT64     | The calculated size of the column in bytes.         |

## Known Limitations

- **Range Partitioning**: Currently, tables using integer range partitioning are treated as non-partitioned tables.
- **Legacy SQL**: Some internal queries may use Legacy SQL syntax for partition discovery.

## License

This project is licensed under the [LICENSE](LICENSE) file included in the repository.
