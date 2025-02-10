# Redshift ETL Music Streaming Project

This project implements a data warehouse solution using Amazon Redshift for a music streaming service. It processes and loads song and user activity data from S3 into a star schema optimized for song play analysis.

## Project Overview

The project creates an ETL pipeline that extracts data from S3, stages it in Redshift, and transforms it into a set of dimensional tables. This enables analytics teams to find insights into what songs users are listening to.

### Database Schema

#### Staging Tables

- `staging_events`: Raw event data from user activity logs
- `staging_songs`: Raw song metadata

#### Fact Table

- `fact_songplays`: Records of song plays including user, song, artist, and time information

#### Dimension Tables

- `dim_users`: User information
- `dim_songs`: Song information
- `dim_artists`: Artist information
- `dim_time`: Timestamps broken down into specific time units

## How to Run

1. Configure AWS credentials:

   - Create a `dwh.cfg` file using the provided `dwh.cfg.example` template
   - Fill in your Redshift cluster details, IAM role ARN, and S3 bucket information

2. Create the required tables:

   ```bash
   python create_tables.py
   ```

   This script creates fresh tables.

3. Run the ETL pipeline:
   ```bash
   python etl.py
   ```
   This loads data from S3 into staging tables and then transforms it into the analytics tables.

## Files in the Repository

- `create_tables.py`: Script to create and reset the database tables
- `etl.py`: Script to extract data from S3, load into staging tables, and insert into analytics tables
- `sql_queries.py`: Contains all SQL queries used in the ETL process
- `dwh.cfg.example`: Template for configuration file (create a copy named `dwh.cfg` with your credentials)

## Requirements

- Python 3.x
- psycopg2
- configparser
- AWS Redshift cluster
- AWS IAM role with proper S3 read access

## Note

Ensure your Redshift cluster is properly configured and accessible before running the scripts. The IAM role should have appropriate permissions to read from the S3 buckets specified in the configuration.
