import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import json
import os

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# Path to the folder containing input files
folder_path = 'data/1st_test/1st_test'

# Function to read columns from a file
def read_columns(file_path):
    with open(file_path, "r") as file:
        for line in file:
            columns = line.split()  # Split the line into columns
            yield columns

# Function to send columns to the consumer
def send_columns(columns, filename):
    x = 0
    for column in columns:
        producer.send('bearingdata', value=column)
        x += 1
        print(f"{filename} Line {x} sent to the consumer:", column)
        ## add 1 second delay
        sleep(1)

# Check if the folder exists
if not os.path.exists(folder_path):
    print("The folder does not exist.")
else:
    # Iterate over all files in the folder
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)

        if os.path.isfile(file_path):
            print("Processing file:", filename)
            columns = read_columns(file_path)
            send_columns(columns, filename)

producer.close()