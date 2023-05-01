import apache_beam as beam
import argparse
import json
import logging
import os
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Set credentials to access Google Cloud Project
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'

class ParseData(beam.DoFn):
    "Custom Class to apply bytes to string transformation"

    def process(self, element):
        # Define the columns based on schema
        columns = [
            'timestamp', 'brand', 'product_name', 'category', 'price',
            'warehouse', 'supplier_id', 'stock', 'defective', 'available'
        ]

        # Convert bytes into string and turn it into list
        row = element.decode('utf-8').split(';')
        data = dict()

        # Add column as keys to the data
        for key, val in zip(columns, row):
            if val == '':
                data[key] = None
            else:
                data[key] = val

        # Wrap in a list since we must return an iterable object 
        return [data]


if __name__ == '__main__':

    logging.basicConfig(format = '%(message)s', level = logging.INFO)

    # Set script to accept command line arguments
    parser = argparse.ArgumentParser(
        prog = 'Warehouse Inventory Sensor Simulator',
        description = 'Simulate real-time sensor behavior, sending data to Google Cloud Pub/Sub'
    )
    
    parser.add_argument(
        '-sub', '--subscription',
        help = 'Pub/Sub subscription ID',
        required = True
    )

    parser.add_argument(
        '-p', '--project',
        help = 'Designated Project ID',
        required = True
    )

    parser.add_argument(
        '-o', '--output',
        help = 'BigQuery Output Table in the format of <dataset_name>.<table_name>',
        required = True
    )

    parser.add_argument(
        '-t', '--temp',
        help = 'Cloud Storage bucket URI for temporary location',
        required = True
    )

    args, extra_args = parser.parse_known_args()

    # Create DataFlow Pipeline and its steps
    options = PipelineOptions(
        extra_args,
        temp_location = args.temp,
        project = args.project,
        streaming = True
        )
    options = options.view_as(StandardOptions)

    with beam.Pipeline(options = options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
                subscription = f'projects/{args.project}/subscriptions/{args.subscription}'
            ).with_output_types(bytes)

            | "BytesToString" >> beam.ParDo(ParseData())

            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table = f'{args.project}:{args.output}',
                write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )