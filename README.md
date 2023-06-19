# Local Data Warehouse using Docker

Extract the data from a relational databse, load in another database in a schema only for the staging area, then load into a final schema for the Data Warehouse, in the same database of the staging area, but in another schema. Each database will be a separated container.
For the source, I use a db called `source_db`, in the schema `dbs`;
For the staging area and DW i used a db called `destiny_db`, in the schemas `sta` and `dw`

## Objective

Do the entire process of the Data Warehouse for a industrial fictional company called "TechFab S.A.". this company need to get reports for all sectors: production, sales, suppliers, financial, maintenance and quality. This reports and data will be used to take Data Driven Decision to turn TechFab a even bigger company.

TechFab S.A. is a big company, with more than 700 employees, with two industries, a big warehouse and 23 shops arround Brazil

For me, this will be the first Data Warehouse the first data warehouse that I do entirely from the bottom start. Since the business problem identification, data modelling, until the final ETL process and queries to generate the reports needed.

## Pre requisites

- Python;
- Apache Airflow;
- PostgreSQL;
- Docker

**TODO**

## Repository Structure

-`/sql/` all SQL scripts used;

**TODO**

## About the project

All the data used in this project are fictional, for practice and research purposes only.

This project is made by part of the course "Design e Implementação de Data Warehouses" avaliable at https://www.datascienceacademy.com.br/

## DataWarehouse modelling

#### Dimensions Tables

- D_TIME;
- D_PRODUCT;
- D_LOCALE;
- D_CUSTOMER.

#### Fact Table

- F_SALES;

## License

This project is licensed under the MIT License - see the [MIT LICENSE](LICENSE) file for details.
