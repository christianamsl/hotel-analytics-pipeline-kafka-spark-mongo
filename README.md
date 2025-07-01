
# Hotel Booking Real-Time Data Pipeline

A real-time data pipeline for processing hotel booking activity using Apache Kafka, Apache Spark Structured Streaming, and MongoDB. The system ingests user interaction data from a hotel booking website, processes it in real time, and stores both raw and aggregated results for analysis.

## Overview

This project demonstrates a complete streaming data architecture:

- Simulated clickstream data generation
- Kafka-based event streaming
- Real-time processing with Spark
- Storage of raw and aggregated data in MongoDB
- Querying for analytics and insights

## Technologies

- Apache Kafka – Event streaming
- Apache Spark Structured Streaming – Real-time processing
- MongoDB – Document storage
- Python – Integration and scripting

## Project Structure

```
producer.py             # Kafka producer for sending events
consumer.py             # Kafka consumer for monitoring
kafka_spark_stream.py   # Spark streaming job
query_mongo.py          # Queries on MongoDB
hotel_sim.py            # CSV data generator
hotel_clickstream.csv   # Simulated user activity
```

## How to Run

1. Start Kafka and create the topic
2. Run the producer and consumer
3. Run the Spark streaming job 
4. Run the MongoDB query script
   

## Insights Provided

- Top destination by number of bookings
- Top destination by number of searches
- Average stay duration per city

## Author

Christiana Mousele  
University of Patras  
Academic Year: 2024/25
