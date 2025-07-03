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
CREATE DATABASE demo;

-- Connect to the demo database
\connect demo

-- Create raw schema and tables for storing raw JSON data
CREATE SCHEMA raw;
CREATE TABLE raw.customer (data JSONB NOT NULL);
CREATE TABLE raw.account (data JSONB NOT NULL);
CREATE TABLE raw.transaction (data JSONB NOT NULL);

-- Create stage schema for data cleansing and transformations
CREATE SCHEMA stage;

-- Create curate schema for serving curated data for reporting and analysis
CREATE SCHEMA curate;