-- Run once in a Snowflake trial account (Snowsight worksheet, role ACCOUNTADMIN).
-- Creates an isolated role, an XSMALL warehouse that suspends after 60 s, a database and a
-- key-pair service user for the rebuild job. Replace the public key placeholder first.
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS GLP_LOADER;
CREATE WAREHOUSE IF NOT EXISTS GLP_WH WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
CREATE DATABASE IF NOT EXISTS GLP;
CREATE SCHEMA IF NOT EXISTS GLP.MARTS;

GRANT USAGE ON WAREHOUSE GLP_WH TO ROLE GLP_LOADER;
GRANT USAGE ON DATABASE GLP TO ROLE GLP_LOADER;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA GLP.MARTS TO ROLE GLP_LOADER;

-- Public key only (the body between the BEGIN/END lines of glp_key.pub). The private key never
-- leaves your machine except into the Kubernetes Secret.
CREATE USER IF NOT EXISTS GLP_SERVICE
  TYPE = SERVICE
  DEFAULT_ROLE = GLP_LOADER
  DEFAULT_WAREHOUSE = GLP_WH
  DEFAULT_NAMESPACE = GLP.MARTS
  RSA_PUBLIC_KEY = '<paste public key body here>';
GRANT ROLE GLP_LOADER TO USER GLP_SERVICE;
-- Put the role under SYSADMIN (Snowflake's recommended hierarchy). Without this, even ACCOUNTADMIN
-- cannot read the tables and views the service user creates.
GRANT ROLE GLP_LOADER TO ROLE SYSADMIN;

-- Guardrail for a trial: stop the warehouse after 1 credit a month.
CREATE RESOURCE MONITOR IF NOT EXISTS GLP_MONITOR WITH CREDIT_QUOTA = 1
  FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE GLP_WH SET RESOURCE_MONITOR = GLP_MONITOR;
