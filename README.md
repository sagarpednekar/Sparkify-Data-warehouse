# Data Warehouse Project README
![Alt text](https://file%2B.vscode-resource.vscode-cdn.net/Users/sagarpednekar/Downloads/sparkify-Page-1.drawio.png?version%3D1692485675306)
## Project: Sparkify Data Warehouse

![License](https://img.shields.io/badge/license-MIT-blue.svg)

## Introduction

Sparkify, a music streaming startup, aims to migrate their data and processes to the cloud. This project involves building an ETL pipeline to extract data from S3, stage it in Redshift, and transform it into a set of dimensional tables for analytical insights.

## Project Description

The project involves developing an ETL pipeline on Amazon Redshift to load data from S3, create staging tables, and transform the data into a star schema optimized for song play analysis. The goal is to enable Sparkify's analytical team to uncover insights about user behavior and music preferences.

## Project Datasets

The project utilizes three datasets stored in Amazon S3:

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data
- Log JSON Path: s3://udacity-dend/log_json_path.json

## Schema for Song Play Analysis

The project's schema consists of both fact and dimension tables:

**Fact Table**
- `songplays`: Records related to song plays
  - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**
- `users`: Information about app users
  - user_id, first_name, last_name, gender, level
- `songs`: Details about songs
  - song_id, title, artist_id, year, duration
- `artists`: Details about artists
  - artist_id, name, location, latitude, longitude
- `time`: Timestamps of songplays broken down
  - start_time, hour, day, week, month, year, weekday

## Project Setup Checklist

Before starting the project, ensure you have the necessary resources ready:

- Redshift cluster is set up
- IAM role with S3 read access is created
- Database and IAM role information is added to `dwh.cfg`

## Project Steps

1. **Create Table Schemas**
   - Design fact and dimension table schemas
   - Write SQL CREATE statements for tables in `sql_queries.py`
   - Complete `create_tables.py` to connect to the database and create tables
   - Add SQL DROP statements to reset tables if necessary
   - Launch Redshift cluster and IAM role with S3 read access
   - Test by running `create_tables.py` and verifying schemas in Redshift

2. **Build ETL Pipeline**
   - Implement logic in `etl.py` to load data from S3 to staging tables on Redshift
   - Implement logic in `etl.py` to transform data from staging tables to analytics tables
   - Test by running `etl.py` after `create_tables.py` and compare results to expectations
   - Delete Redshift cluster when done


## Conclusion

This project enables Sparkify to leverage the power of Amazon Redshift for analyzing user behavior and music trends. By designing a star schema and implementing a robust ETL pipeline, insights can be gained to enhance the user experience and drive business decisions.



