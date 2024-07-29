CREATE SCHEMA IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.raw_data
(
    "RAW_ID" bigint,
    "RECORD_METADATA" json,
    "RECORD_DATA" json
);

CREATE TABLE IF NOT EXISTS demo.stage_customer
(
    "RAW_ID" bigint,
    "STAGE_ID" bigint,
    "CUSTOMER_NO" varchar(10),
    "FIRST_NAME" varchar(200),
    "LAST_NAMES" varchar(200),
    "PHYSICAL_ADDRESS" varchar(500),
    "POSTAL_ADDRESS" varchar(500),
    "PHONE_NUMBER" varchar(20),
    "RECORD_HASH" varchar(50)
);

CREATE TABLE IF NOT EXISTS demo.stage_account
(
    "RAW_ID" bigint,
    "STAGE_ID" bigint,
    "ACCOUNT_NO" varchar(20),
    "ACCOUNT_TYPE" char(1),
    "ACCOUNT_NAME" varchar(200),
    "CUSTOMER_NO" varchar(10),
    "RECORD_HASH" varchar(50)
);

CREATE TABLE IF NOT EXISTS demo.dim_customer
(
    "RAW_ID" bigint,
    "STAGE_ID" bigint,
    "DIM_ID" bigint,
    "DIM_VALID_FROM" timestamp,
    "DIM_VALID_TO" timestamp,
    "CUSTOMER_NO" bigint,
    "FIRST_NAME" varchar(200),
    "LAST_NAMES" varchar(200),
    "PHYSICAL_ADDRESS" varchar(500),
    "POSTAL_ADDRESS" varchar(500),
    "PHONE_NUMBER" varchar(20),
    "RECORD_HASH" varchar(50)
);

CREATE TABLE IF NOT EXISTS demo.dim_account
(
    "RAW_ID" bigint,
    "STAGE_ID" bigint,
    "DIM_ID" bigint,
    "DIM_VALID_FROM" timestamp,
    "DIM_VALID_TO" timestamp,
    "ACCOUNT_NO" varchar(20),
    "ACCOUNT_TYPE" char(1),
    "ACCOUNT_NAME" varchar(200),
    "CUSTOMER_NO" varchar(10),
    "RECORD_HASH" varchar(50)
);