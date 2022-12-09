
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
CREATE TABLE kf_fantom_blocks
(
   base_fee_per_gas decimal(38, 0),
   transaction_count int,
   `timestamp` bigint,
   gas_used decimal(38, 0),
   gas_limit decimal(38, 0),
   extra_data STRING,
   size decimal(38, 0),
   total_difficulty decimal(38, 0),
   difficulty decimal(38, 0),
   miner varchar,
   receipts_root varchar,
   state_root varchar,
   transactions_root varchar,
   logs_bloom STRING,
   sha3_uncles varchar,
   nonce varchar,
   parent_hash varchar,
   hash STRING,
   number int
) WITH (
    'connector' = 'kafka',
    'topic' = 'chain_data_fantom_blocks',
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
SET 'pipeline.name' = 'fantom_blocks_sync';


-- submit job
insert into iceberg.beta_bronze.fantom_blocks
select
    base_fee_per_gas,
    transaction_count,
    to_timestamp(from_unixtime(`timestamp`, 'yyyy-MM-dd HH:mm:ss')) as `timestamp`,
    gas_used,
    gas_limit,
    extra_data,
    size,
    total_difficulty,
    difficulty,
    miner,
    receipts_root,
    state_root,
    transactions_root,
    logs_bloom,
    sha3_uncles,
    nonce,
    parent_hash,
    hash,
    number
from kf_fantom_blocks;