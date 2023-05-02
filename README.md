# Building a Streaming Pipeline on Google Cloud Platform

This repository contains the data and scripts used in the [Supertype article](https://supertype.ai/notes/streaming-pipeline-for-warehouse-inventory-management-system) with the same name, with the main contents being:

- Creating a sensor simulation
- Integrating the simulated sensor into Google Cloud Pub/Sub
- Building a streaming Dataflow pipeline to ingest data from Pub/Sub into BigQuery

## About the Data

The warehouse inventory data in this repository is an artificially made, highly-compressed CSV files. The data consists of around 16 million rows and 10 columns, with the columns being:

- *timestamp*: the timestamp in which the product is recorded by the sensor
- *brand*: product brand
- *product_name*: name of the product
- *category*: product category
- *price*: product price in USD
- *warehouse*: the warehouse name that stores the product
- *supplier_id*: ID of the supplier, NULL if the product is not resupplied
- *stock*: the number of product stocks
- *defective*: the number of defective stocks
- *available*: the number of non-defective stocks

The rows in the dataset are sorted by the `timestamp` to simulate a sensor. Here are some sample rows from the dataset

```
timestamp;brand;product_name;category;price;warehouse;supplier_id;stock;defective;available
2023-01-01 00:00:00;Apple;Mac Mini;Desktop PC;599.0;Electra;;110;0;110
2023-01-01 00:00:00;Apple;Mac Mini;Desktop PC;599.0;Tesla;;62;0;62
2023-01-01 00:00:00;Apple;Mac Mini;Desktop PC;599.0;Volta;;63;0;63
```

## How to Use the Script

To use the script, create a virtual environment in your local machine and run the following command to install the dependencies

```{bash}
pip install -r requirements.txt
```

Create a service account on your Google Cloud Project and give it the **Editor** and **Pub/Sub Editor** roles. Create a service account key in JSON format and save it as `credentials.json` in the same directory as the Python scripts. The `simulate_sensor.py` can be used to simulate a sensor that will continuously send data into your Google Pub/Sub topic having the ID of `inventory-streaming` (you can change the topic name by changing the `_topic` variable value in the script). Use the following command upon creating the Pub/Sub topic

```{bash}
python3 simulate_sensor.py \
    --speedMultiplier 1 \
    --project <your_project_id>
```

> the speed multiplier is used to determine how many minutes of data will be sent into Pub/Sub in 1 minute realtime

Use `dataflow_pubsub_to_bq.py` to deploy a streaming pipeline into Dataflow. The pipeline will continously ingest any message from a designated Pub/Sub subscription into the BigQuery table.

```{bash}
python -m dataflow_pubsub_to_bq \
    --subscription <subscription_id> \
    --project <project_id> \
    --output <dataset_name>.<table_name> \
    --temp <gcs_uri> \
    --runner DataflowRunner \
    --region <closest_region> \
    --num_workers 1 \
    --max_num_workers 5 \
    --job_name pubsub-to-bigquery
```