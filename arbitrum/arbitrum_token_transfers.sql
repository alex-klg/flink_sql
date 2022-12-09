
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
CREATE TABLE kf_arbitrum_token_transfers
(
   token_address varchar,
   from_address varchar,
   to_address varchar,
   `value` varchar,
   amount_raw as TRY_CAST(`value` AS DECIMAL(38,0)),
   transaction_hash varchar,
   log_index bigint,
   block_number bigint,
   block_timestamp bigint,
   block_timestamp_p as to_timestamp(from_unixtime(block_timestamp, 'yyyy-MM-dd HH:mm:ss')),
   block_hash varchar
) WITH (
    'connector' = 'kafka',
    'topic' = 'chain_data_arbitrum_token_transfers',
    'properties.bootstrap.servers' = '10.202.0.114:9092',
    'properties.group.id' = 'flink_read_group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);


-- iceberg table 在 trino 那边建好

-- set checkpointing 写入 iceberg 的时间间隔
SET 'execution.checkpointing.interval' = '4min';
SET 'pipeline.name' = 'arbitrum_token_transfers_sync';


-- submit job
insert into iceberg.beta_bronze.arbitrum_token_transfers
select
token_address,
from_address,
to_address,
`value`,
transaction_hash,
log_index,
block_timestamp_p as block_timestamp,
block_number,
block_hash,
amount_raw
from kf_arbitrum_token_transfers;