# TechFab Data Warehouse

## Project Overview

The TechFab Data Warehouse project aims to build a local data warehouse using Docker for TechFab S.A., a fictional industrial company. The data warehouse will enable data-driven decision-making across various sectors such as production, sales, suppliers, finance, maintenance, and quality. By leveraging the power of a data warehouse, TechFab S.A. aims to enhance operational efficiency, identify growth opportunities, and drive the company's success.

## Architeture Explanaition

The data warehouse follows a multi-step process, involving the extraction of data from a relational database, loading it into a staging area, and then loading it into the final data warehouse. Each step is performed in separate Docker containers. The source database is named source_db, located in the dbs schema. The staging area and the data warehouse reside in the destiny_db, with the staging area in the sta schema and the data warehouse in the dw schema.

For the Extract and Load step, Apache Airflow with PostgreSQL and Python operators is utilized. The Airflow DAG, running within a Docker container, extracts data from the source DB and stores it in the staging area DB. The transformation and final loading into the data warehouse are performed using SQL scripts.

## Objective

Do the entire process of the Data Warehouse for a industrial fictional company called "TechFab S.A.". this company need to get reports for all sectors: production, sales, suppliers, financial, maintenance and quality. This reports and data will be used to take Data Driven Decision to turn TechFab a even bigger company.

TechFab S.A. is a big company, with more than 700 employees, with two industries, a big warehouse and 23 shops arround Brazil

For me, this will be the first Data Warehouse the first data warehouse that I do entirely from the bottom start. Since the business problem identification, data modelling, until the final ETL process and queries to generate the reports needed.

## Pre requisites

- PostgreSQL;
- Docker
- Python;
- Airflow (the Docker version: `https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html`)

## Repository Structure

- `/sql/` all SQL scripts used;

  - `dw_` stands for Data Warehouse scripts;
  - `sr_` is for the source tables;
  - `sta_` is for the staging area scripts;
  - `/etl/` scripts used to do the ETL process in the staging area to the dw;
  - `/change_granularity/` SQL scripts to change time granularity after the DW implementation;

- `/dags/` Apache Airflow DAG to transfer data between source_db an stagin area in destiny_db;
  Note that the DAG does not include the script to load the source database tables, as it is not logical to do so.

  ## Project Objective

The main objective of this project is to develop a comprehensive data warehouse solution for TechFab S.A. This data warehouse will provide insights and reports for all sectors of the company, including production, sales, suppliers, finance, maintenance, and quality. By leveraging the data and reports generated, TechFab S.A. aims to make data-driven decisions to propel the company's growth and success.

## DataWarehouse modelling

This Data Warehouse uses the Star Schema, with 4 dimensions table and one fact table, for the sales.

#### Dimensions Tables

- D_TIME;
- D_PRODUCT;
- D_LOCALE;
- D_CUSTOMER.

#### Fact Table

- F_SALES;

## Usage instructions

- Clone this repository to your local machine;
- Install the required dependencies (PostgreSQL, Docker, Python, and Airflow);
- Set up the PostgreSQL databases for the source, staging area, and data warehouse;
- Run the source_db initial load in pgAdmin;
- Configure the Airflow DAG to connect to the source and staging area databases;
- Execute the Airflow DAG to perform the Extract and Load process;
- Generate reports and perform data analysis using SQL queries on the data warehouse;

## About the project

All the data used in this project are fictional, for practice and research purposes only.

This project is made by part of the course "Design e Implementação de Data Warehouses" avaliable at https://www.datascienceacademy.com.br/

## License

This project is licensed under the MIT License - see the [MIT LICENSE](LICENSE) file for details.
