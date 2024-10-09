import csv
import json
import random
from time import sleep

from pandas.core.arrays.arrow.array import cast_for_truediv
from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment,
    DataTypes,
    TableDescriptor,
    Schema
)

from config import config
from confluent_kafka import Producer

# csv_file='/home/sreenivas/work/poc/healthcare_kafka_flink/input_data/health_data_large.csv'
csv_file= '/home/sreenivas/work/poc/data/b.csv'

producer = Producer(config)
topic = 'patient-health'
hospitalGroupIds=[1,2,3,4,5,6,7,8,9,10]

def process_csv():
    # read csv file
    with open(csv_file, 'r') as file:
        counter = 0
        reader = csv.DictReader(file)
        for row in reader:
            counter += 1
            record = {}

            for key, value in row.items():
                if key == 'age' : record[key] = int(value)
                else : record[key] = value

            record['healthGroupId'] = "HGID-" + str(random.choice(hospitalGroupIds))
            print(counter)
            write_to_kafka(record)
        print(f"total records inserted: {counter}")
            #sleep(.5)

def write_to_kafka(record):
    producer.produce(topic=topic, key=record['healthGroupId'], value=json.dumps(record))

if __name__ == "__main__" :
    try:
        process_csv()
    except Exception as e:
       print(e)
    finally:
        producer.flush()