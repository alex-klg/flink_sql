
-- Define available catalogs
CREATE CATALOG iceberg WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'uri'='thrift://10.106.192.22:9083',
  'clients'='5',
  'property-version'='1',
  'warehouse'='gs://footbase-prod/hive-warehouse'
);

-- Define table
-- 在 trino 执行 show ca
CREATE TABLE kf_arbitrum_transactions
(
   hash STRING,
   nonce bigint,
   transaction_index bigint,
   from_address varchar,
   to_address STRING,
   `value` decimal(38, 0),
   gas bigint,
   gas_price bigint,
   input STRING,
   receipt_cumulative_gas_used bigint,
   receipt_gas_used bigint,
   receipt_contract_address varchar,
   receipt_root varchar,
   receipt_status bigint,
   `block_timestamp` bigint,
   block_number bigint,
   block_hash varchar,
   max_fee_per_gas bigint,
   max_priority_fee_per_gas bigint,
   transaction_type bigint,
   receipt_effective_gas_price bigint
) WITH (
    'connector' = 'kafka',
    'topic' = 'chain_data_arbitrum_transactions',
    'properties.bootstrap.servers' = '10.202.0.114:9092',
    'properties.group.id' = 'flink_read_group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

SET 'execution.checkpointing.interval' = '4min';
SET 'pipeline.name' = 'arbitrum_transactions_sync';

insert into iceberg.beta_bronze.arbitrum_transactions
select
    hash, nonce, transaction_index,
    from_address, to_address, `value`,
    gas, gas_price, `input`,
    receipt_cumulative_gas_used,
    receipt_gas_used, receipt_contract_address,
    receipt_root, receipt_status,
    to_timestamp(from_unixtime(block_timestamp, 'yyyy-MM-dd HH:mm:ss')) as block_timestamp,
    block_number, block_hash,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type, receipt_effective_gas_price
from kf_arbitrum_transactions;