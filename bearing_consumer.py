from kafka import KafkaConsumer
import json

# Kafka consumer configuration
consumer = KafkaConsumer('bearingdata',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my-group',
                         value_deserializer=lambda x: x.decode('utf-8'))

# Path to the file where data will be stored
output_file = 'output.txt'

# Function to write received data to a file
def write_to_file(data):
    with open(output_file, 'a') as file:
        file.write(data + '\n')

# Iterate over received messages from the topic
for message in consumer:
    # Get the value of the message (data sent by the producer)
    data = message.value
    # Write the data to the file
    write_to_file(data)
    print("Data written to file:", data)

# Close the consumer after finishing reading
print("All rows received and saved to received_rows.csv")
consumer.close()
