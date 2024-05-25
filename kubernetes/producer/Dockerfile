# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app
RUN apt-get update && apt-get install curl -y;
RUN curl \
  -X POST \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic Q1JXTFdHTUNYTEJKR1g3NDpXZ0tRZWJxUkVWb1RRT2psWElBS0ViSFlkZnkya3R5YVZUYzZ2MXRsb1JWWkJPcml5RHJydlZOT2Q5OVYzVzBX" \
  https://pkc-56d1g.eastus.azure.confluent.cloud:443/kafka/v3/clusters/lkc-3338qj/topics \
  -d '{"topic_name":"sales", "topic_name":"customer"}'
# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run the script
CMD ["python3", "producer.py"]
