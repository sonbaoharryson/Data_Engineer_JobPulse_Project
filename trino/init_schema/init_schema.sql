CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location='s3://warehouse/bronze');
CREATE SCHEMA IF NOT EXISTS iceberg.audit
WITH (location='s3://warehouse/audit');
CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location='s3://warehouse/silver');
CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location='s3://warehouse/gold');
