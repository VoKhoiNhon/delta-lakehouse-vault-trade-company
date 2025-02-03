-- Create the first user and database
CREATE USER trino WITH PASSWORD 'trino_password';
CREATE DATABASE hive_metastore OWNER trino;


-- Grant all privileges on the databases to their respective users
GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO trino;

