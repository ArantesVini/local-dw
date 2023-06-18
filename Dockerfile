# Dockerfile for creating PostgreSQL containers

# Base image
FROM postgres:latest

# Set environment variables for PostgreSQL
ENV POSTGRES_USER=dbadmin
ENV POSTGRES_PASSWORD=dbadmin123
ENV POSTGRES_DB=postgresDB

# Expose the default PostgreSQL port
EXPOSE 5432

# Command to create the source database container
CMD ["postgres"]

# To run use the following commands:
# docker build -t source-db .
# docker run -d --name source_db -p 5433:5432 source-db
# docker build -t destiny-db .
# docker run -d --name destiny_db -p 5434:5432 destiny-db