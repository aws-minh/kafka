# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the client.properties file into the container
COPY client.properties /app/client.properties

# Make port 9092 available to the world outside this container
EXPOSE 9092

# Run consumer.py when the container launches
CMD ["python", "consumer.py"]
