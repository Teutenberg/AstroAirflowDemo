-- ============================================================================
-- demo_db_create.sql
--
-- This script is used by the Docker Compose override to initialize the demo
-- PostgreSQL database. It creates the DEMO database, schemas, and required tables
-- for the Airflow and dbt demo environment.
--
-- Usage: Automatically executed by the Docker override on container startup.
--        Can also be run manually in psql for local development/testing.
-- ============================================================================

-- Create a new database named demo
CREATE DATABASE DEMO;
-- Connect to the demo database
-- Note: The \connect command is specific to psql, the PostgreSQL command-line interface
\connect DEMO
-- Create raw schema and a table in the demo database
CREATE SCHEMA RAW;
CREATE TABLE RAW.CUSTOMER (DATA JSONB NOT NULL);
CREATE TABLE RAW.ACCOUNT (DATA JSONB NOT NULL);
CREATE TABLE RAW.TRANSACTION (DATA JSONB NOT NULL);

CREATE SCHEMA STAGE;