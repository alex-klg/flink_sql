
-- Define available catalogs
CREATE CATALOG iceberg WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'uri'='thrift://10.106.192.22:9083',
  'clients'='5',
  'property-version'='1',
  'warehouse'='gs://footbase-prod/hive-warehouse'
);

-- 在 trino 执行 show create table 就可以拿到建表语句了
CREATE TABLE kf_fantom_logs
(
   log_index bigint,
   transaction_hash varchar,
   transaction_index bigint,
   address varchar,
   data varchar,
   topics array<string>,
   block_timestamp bigint,
   block_timestamp_p as to_timestamp(from_unixtime(block_timestamp, 'yyyy-MM-dd HH:mm:ss')),
   block_number bigint,
   block_hash varchar
) WITH (
    'connector' = 'kafka',
    'topic' = 'chain_data_fantom_logs',
    'properties.bootstrap.servers' = '10.202.0.114:9092',
    'properties.group.id' = 'flink_read_group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- iceberg table 在 trino 那边建好

SET 'execution.checkpointing.interval' = '4min';
SET 'pipeline.name' = 'fantom_logs_sync';


-- submit job
insert into iceberg.beta_bronze.fantom_logs
select
    log_index, transaction_hash,
    transaction_index, address, `data`,
    topics,
    block_timestamp_p as block_timestamp,
    block_number, block_hash
from kf_fantom_logs;