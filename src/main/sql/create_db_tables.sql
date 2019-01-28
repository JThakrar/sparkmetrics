
CREATE TABLE app_metric(
  app_id VARCHAR,
  app_name VARCHAR,
  app_user VARCHAR,
  start_time_epoch BIGINT,
  end_time_epoch BIGINT,
  duration BIGINT,
  start_date_time TIMESTAMP,
  end_date_time TIMESTAMP
) ;

CREATE TABLE job_metric(
  app_id VARCHAR,
  app_name VARCHAR,
  app_user VARCHAR,
  job_id INT,
  start_time_epoch BIGINT,
  end_time_epoch BIGINT,
  duration BIGINT,
  start_date_time TIMESTAMP,
  end_date_time TIMESTAMP,
  stages VARCHAR
) ;

CREATE TABLE stage_metric(
  app_id VARCHAR,
  app_name VARCHAR,
  app_user VARCHAR,
  job_id INT,
  stage_id INT,
  start_time_epoch BIGINT,
  end_time_epoch BIGINT,
  duration BIGINT,
  start_date_time TIMESTAMP,
  end_date_time TIMESTAMP,
  name VARCHAR,
  details VARCHAR,
  num_tasks INT,
  parent_ids VARCHAR,
  attempt_number INT,
  failure_reason VARCHAR,
  disk_bytes_spilled BIGINT,
  executor_cpu_time BIGINT,
  executor_deserialize_cpu_time BIGINT,
  executor_deserialize_time BIGINT,
  executor_run_time BIGINT,
  bytes_read BIGINT,
  records_read BIGINT,
  jvm_gc_time BIGINT,
  memory_bytes_spilled BIGINT,
  bytes_written BIGINT,
  records_written BIGINT,
  peak_execution_memory BIGINT,
  result_serialization_time BIGINT,
  result_size BIGINT,
  shuffle_fetch_wait_time BIGINT,
  shuffle_local_blocks_fetched BIGINT,
  shuffle_local_bytes_read BIGINT,
  shuffle_records_read BIGINT,
  shuffle_remote_blocks_fetched BIGINT,
  shuffle_remote_bytes_read BIGINT,
  shuffle_remote_bytes_read_to_disk BIGINT,
  shuffle_bytes_written BIGINT,
  shuffle_records_written BIGINT,
  shuffle_write_time BIGINT
) ;

