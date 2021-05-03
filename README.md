# BQ Column sizes

[![](https://jitpack.io/v/Kayrnt/bq_column_sizes.svg)](https://jitpack.io/#Kayrnt/bq_column_sizes)

This project is meant to allow extract column sizes from BigQuery tables to monitor fields weight changes over time.

## How it works

The job will scan selected GCP project / datasets / tables then scan for partitions to launch a select as dry run per field and per partition to store the bytes that would be read.

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
  --usage  <bool>
        Print usage and exit
  --help | -h  <bool>
        Print help message and exit
  --execution-project-id | --ep  <string?>
        GCP project id used for query execution if different from read.
  --project-id | -p  <string>
        GCP project id to analyze, if no dataset is provided, it will analyze all datasets for that project.
  --dataset | -d  <string?>
        dataset to analyze, if no table is provided it will analyze all tables for that dataset.
  --table | -t  <string?>
        table to analyze
  --output-file-path | -o  <string?>
        output file path, default in size.csv, you can use an absolute path such as /Users/myUser/size.csv.
  --max-concurrent-queries | --mcq  <int?>
        max concurrent queries as you're throttled on actual BQ requests, default to 4.
  --output-writer | --ow  <string?>
        output writer such as bq or csv, default to csv
  --output-project | --op  <string?>
        Bigquery output project when bq output type is selected, default to the projectId field.
  --output-dataset | --od  <string?>
        Bigquery output dataset when bq output type is selected
  --output-table | --ot  <string?>
        Bigquery output table when bq output type is selected
  --partition | --pt  <string?>
        Partition to analyze, if not specified all partitions will be analyzed.
Format: yyyy, yyyyMM, yyyyMMdd, yyyyyMMddHH
Example: 20210101

```

## Examples

For instance, following command allows to get the column sizes from `bigquery-public-data.crypto_bitcoin.blocks` table on partition `20210501` and output the result in table `my_gcp_project.my_dataset.my_bq_column_sizes`:
```
bq-column-sizes -p bigquery-public-data -d crypto_bitcoin -t blocks --ow bq --ot my_gcp_project --od my_dataset --ot my_bq_column_sizes --pt 20210501
```

If you would like to store it in a local CSV file, you can use simply use:
```
bq-column-sizes -p bigquery-public-data -d crypto_bitcoin -t blocks --pt 20210501
```