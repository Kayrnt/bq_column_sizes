# BQ Column sizes

[![](https://jitpack.io/v/Kayrnt/bq_column_sizes.svg)](https://jitpack.io/#Kayrnt/bq_column_sizes)

This project is meant to allow extract column sizes from BigQuery tables to monitor fields weight changes over time.

## How it works

The job will scan selected GCP project / datasets / tables then scan for partitions to launch a select as dry run per field and per partition to store the bytes that would be read.

### Output

output data columns:
- **job_partition**: String => the input partition formatted with the job frequency
- **project_id**: String => the GCP project id of the read table
- **dataset**: String => the dataset of the read table,
- **table**: String => the table name of the read table,
- **field**: String => the analyzed field name of the red table,
- **table_partition**: String => the analyzed time partition of the read table,
- **size_in_bytes**: Long => For the selected field & partition, the size in bytes

#### CSV
CSV output will create a dedicated file (or overwrite it if existing) that contains the job run will contain the header and the data per line.

#### BigQuery

BigQuery output will create a dedicated partitioned table if not existing to store the output per partition (based on job_frequency parameter or else daily).


## How to launch

There is multiple ways to start the job:
- Clone the source code a build the project to run the `BQColumnSizes` (Main) class
- Install Coursier and fetch the Jitpack package

The Coursier + Jitpack option is likely easier to use:
1. Install [Coursier](https://get-coursier.io/docs/cli-installation)
2. Fetch the dependency from Jitpack 
   ```coursier fetch -r jitpack com.github.Kayrnt:bq_column_sizes:master-SNAPSHOT```
3. Start the job
```coursier launch -r jitpack com.github.Kayrnt:bq_column_sizes:master-SNAPSHOT -- --help```

## Usage

``` 
BQColumnSizes 1.0.0
Usage: bq-column-sizes [options]

Help options:
  --usage     Print usage and exit
  -h, --help  Print help message and exit

Other options:
  --ep, --execution-project-id string?  GCP project id used for query execution if different from read.
  -p, --project-id string               GCP project id to analyze, if no dataset is provided, it will analyze all datasets for that project.
  -d, --dataset string?                 dataset to analyze, if no table is provided it will analyze all tables for that dataset.
  -t, --table string?                   table to analyze
  -o, --output-file-path string?        output file path, default in size.csv, you can use an absolute path such as /Users/myUser/size.csv.
  --mcq, --max-concurrent-queries int?  max concurrent queries as you're throttled on actual BQ requests, default to 4.
  --ow, --output-writer string?         output writer such as bq or csv, default to csv
  --op, --output-project string?        Bigquery output project when bq output type is selected, default to the projectId field.
  --od, --output-dataset string?        Bigquery output dataset when bq output type is selected
  --ot, --output-table string?          Bigquery output table when bq output type is selected
  --pt, --partition string?             Partition to analyze, if not specified all partitions will be analyzed.
Format: yyyy, yyyyMM, yyyyMMdd, yyyyyMMddHH
Example: 20210101
  --jf, --job-frequency string?         Job frequency: it's how often you're going to run the job so that the job know how to partition the data.
Values: year, month, day, hour.
Default to day.
  --offset string?                      Partition offset as duration
Format: <length><unit> based on scala.concurrent.duration.Duration
Example: 1hour, 1d
```

## Examples

For instance, following command allows to get the column sizes from `bigquery-public-data.crypto_bitcoin.blocks` table on partition `20210501` and output the result in table `my_gcp_project.my_dataset.my_bq_column_sizes` (which will be partitioned per day as default):
```
bq-column-sizes -p bigquery-public-data -d crypto_bitcoin -t blocks --ow bq --ot my_gcp_project --od my_dataset --ot my_bq_column_sizes --pt 20210501
```

If you would like to store it in a local CSV file, you can use simply use:
```
bq-column-sizes -p bigquery-public-data -d crypto_bitcoin -t blocks --pt 20210501
```

## Known limitations
- If you're using range partitioning on a read table, that partitioning will be ignored and treated as a table without partitioning. It's done so because the job is meant to be run on regular basis (ie a daily job).  