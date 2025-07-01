-- Create a new database named demo
CREATE DATABASE demo;
-- Connect to the demo database
-- Note: The \connect command is specific to psql, the PostgreSQL command-line interface
\connect demo

-- Create a schema and a table in the demo database
CREATE SCHEMA raw;
CREATE TABLE raw.customer (data VARCHAR(100) NOT NULL);
CREATE TABLE raw.account (data VARCHAR(100) NOT NULL);
CREATE TABLE raw.transaction (data VARCHAR(100) NOT NULL);