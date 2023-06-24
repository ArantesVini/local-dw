# Local Data Warehouse using Docker

Extract the data from a relational databse, load in another database in a schema only for the staging area, then load into a final schema for the Data Warehouse, in the same database of the staging area, but in another schema. Each database will be a separated container.
For the source, I use a db called `source_db`, in the schema `dbs`;
For the staging area and DW i used a db called `destiny_db`, in the schemas `sta` and `dw`.

In the Extract And Load step, to take the data from the source DB and store it at the staging area DB I will use Airflow with postgres and python operators (all running in Docker), for all the Transformation and final load into the DW, I will use SQL.

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
  - `sta_` is for the staging area scripts; -`/etl/` scripts used to do the ETL process in the staging area to the dw;

- `/dags/` Apache Airflow DAG to transfer data between source_db an stagin area in destiny_db;

## DataWarehouse modelling

This Data Warehouse uses the Star Schema, with 4 dimensions table and one fact table, for the sales.

#### Dimensions Tables

- D_TIME;
- D_PRODUCT;
- D_LOCALE;
- D_CUSTOMER.

#### Fact Table

- F_SALES;

## About the project

All the data used in this project are fictional, for practice and research purposes only.

This project is made by part of the course "Design e Implementação de Data Warehouses" avaliable at https://www.datascienceacademy.com.br/

## License

This project is licensed under the MIT License - see the [MIT LICENSE](LICENSE) file for details.
