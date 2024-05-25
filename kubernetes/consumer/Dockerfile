# Use Python base image
FROM python:3.10

# Set the working directory in the container
WORKDIR /app
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean;
#Create Output folder
RUN pip install pyspark
# Copy the Python script into the container
COPY consumer.py /app/consumer.py
COPY driver /app/driver
# Run the Python script
#CMD ["python3", "consumer.py"]
