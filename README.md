# Realtime Streaming Pipeline for Unstructured Data

This repository provides a guide and codebase for building a realtime streaming pipeline capable of handling various unstructured data types such as TEXT, IMAGE, VIDEO, CSV, JSON, and PDF. The pipeline is designed to process over 600+ different datasets in real-time. 

## System Architecture Overview

![](system-architecture.png)

The system architecture utilizes Spark Streaming for real-time data processing. It incorporates various components for data ingestion, processing, transformation, and storage, including AWS S3 for data storage and AWS Glue for data cataloging.

- **Spark Cluster**: Deployed using Docker containers for distributed computation.
- **AWS S3**: Used for storing input data and processed results.
- **AWS Glue**: Employed for cataloging data stored in S3.
- **AWS Athena**: Used for querying and verifying cataloged data.

  
## Getting Started

To set up and run the project locally, follow these steps:

1. Clone this repository to your local machine.
2. Install Docker and Docker Compose if not already installed.
3. Update the configuration parameters in `config/config.py` with your AWS credentials and input/output paths.
4. Run `docker-compose up` to start the Spark cluster.
5. Execute the Spark streaming application by running `python main.py`.
6. Monitor the application's progress through the console output and AWS S3.

## Usage

The main entry point of the project is `main.py`. It sets up the Spark session, defines UDFs, reads data streams, applies transformations, and writes the processed data to the console and S3.

To add additional functionalities or modify existing ones, you can edit the UDFs in `udf_utils.py` or update the Spark application logic in `main.py`.


