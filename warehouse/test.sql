-- UPDATE etl_file_tracker
-- SET status = 'failed', retry_count = retry_count + 1, error_message = 'ERROR!'
-- WHERE run_id = '3d56aafc-36f4-4d09-95d5-d871229ac81f' AND source_table = 'orders';

SELECT source_table, status, retry_count, error_message
FROM etl_file_tracker
WHERE run_id = '3d56aafc-36f4-4d09-95d5-d871229ac81f' AND source_table = 'orders';