import time
import gzip
import logging
import argparse
import os
from datetime import datetime, timedelta
from google.cloud import pubsub

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'

time_format = "%Y-%m-%d %H:%M:%S"
_topic = 'inventory-streaming'
_input = "data/warehouse_inventory.csv.gz"

def get_timestamp(row):
    "Get timestamp (first column) from the CSV row and turn into datetime object"
    row_data = row.decode('utf-8').strip('\n')
    timestamp = row_data.split(';')[0]
    return datetime.strptime(timestamp, time_format)

def peek_timestamp(file):
    "Read the timestamp of a row in the CSV file without going into the next row"

    # save current position & read the row data
    current_position = file.tell()
    row_data = file.readline()

    # get back into the last position and return the timestamp
    file.seek(current_position)
    return get_timestamp(row_data)

def publish(publisher, topic, events):
    "Publish the accumulated events into Pub/Sub Topic"

    event_count = len(events)
    if event_count > 0:
        logging.info(f'Publishing {event_count} events from {get_timestamp(events[0])}')
        for event in events:
            publisher.publish(topic, event)

def simulate(publisher, topic, file, first_event_time, start_time, speed_multiplier):
    """Simulate a sensor by constantly sending data out of a CSV file

    Args:
        file: .csv.gz object. Make sure the CSV header is skipped if exists
        first_event_time: First timestamp in the data, used to compute the program's sleep time
        start_time: Program start time, used to compute the program's sleep time
        speed_multiplier: Speed multiplier for the sensor simulation. Example: 60 implies that 60 minutes of data will be send in 1 minute
    """
    
    def compute_sleep_time(event_time):
        "Compute the necessary sleeping time for the program to adjust to the speed multiplier"

        elapsed_time = (datetime.utcnow() - start_time).seconds
        elapsed_sim_time = ((event_time - first_event_time).days * 86400.0 + (event_time - first_event_time).seconds) / speed_multiplier
        sleep_time = elapsed_sim_time - elapsed_time
        return sleep_time

    # Initiate list of events to be published & last_event_time (will be used to determine whether the row will be published or not)
    events = list() 
    last_event_time = first_event_time

    # Iterate over CSV rows
    for row in file:
        event_data = row.decode('utf-8').strip('\n').encode('utf-8')
        event_time = get_timestamp(row)

        if event_time != last_event_time:
            # Update the last event time
            last_event_time = event_time

            # Publish events
            publish(publisher, topic, events)
            events = list()

            # Compute sleeping time necessary based on the speed multiplier
            sleep_time = compute_sleep_time(event_time)
            logging.info(f'Sleeping {sleep_time} seconds')
            time.sleep(sleep_time)
        
        # Accumulate the event data otherwise
        events.append(event_data)
                
    # Publish left over records if exist
    publish(publisher, topic, events)

if __name__ == '__main__':
    logging.basicConfig(format = '%(message)s', level = logging.INFO)

    parser = argparse.ArgumentParser(
        prog = 'Warehouse Inventory Sensor Simulator',
        description = 'Simulate real-time sensor behavior, sending data to Google Cloud Pub/Sub'
    )
    
    parser.add_argument(
        '-sm', '--speedMultiplier',
        help = 'Speed multiplier, example: 60 implies that 60 minutes of data will be send in 1 minute',
        required = True,
        type = int
    )
    
    parser.add_argument(
        '-p', '--project',
        help = 'Project ID, obtainable with $DEVSHELL_PROJECT_ID',
        required = True,
        type = str
    )
    
    args = parser.parse_args()

    # get Pub/Sub notification topic
    publisher = pubsub.PublisherClient()
    topic_path = publisher.topic_path(args.project, _topic)

    logging.info(f'Using pub/sub {topic_path}')
    
    start_time = datetime.utcnow()

    # Start sensor simulation
    with gzip.open(_input, 'rb') as file:
        # skip header & get first event time
        header = file.readline()
        first_event_time = peek_timestamp(file)
        # begin simulating sensor
        logging.info('Begin simulating sensor data')
        simulate(publisher, topic_path, file, first_event_time, start_time, args.speedMultiplier)