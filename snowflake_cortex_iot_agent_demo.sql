-- set up environment
use role nldemo; -- udpate with role that can create a database
create database if not exists nldemo;
use database nldemo;
create schema if not exists iot_agent_demo;
use schema iot_agent_demo;


-- generate sensor data
CREATE TABLE IF NOT EXISTS IOT_AGENT_DEMO.SENSOR_READINGS AS
WITH time_series AS (
    SELECT 
        DATEADD(HOUR, SEQ4(), '2025-01-01 00:00:00'::TIMESTAMP) AS reading_timestamp,
        SEQ4() AS seq_num
    FROM TABLE(GENERATOR(ROWCOUNT => 7704))
),
customer_data AS (
    SELECT 'CUST-DC-8472' AS customer_id, 'Apex Cloud Data Center' AS customer_name, 18.0 AS min_temp, 21.0 AS max_temp
    UNION ALL
    SELECT 'CUST-PH-3291', 'BioSyn Pharmaceutical Manufacturing', 20.0, 22.0
    UNION ALL
    SELECT 'CUST-AU-5614', 'Precision Automotive Components', 21.0, 24.0
),
cross_joined AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.min_temp,
        c.max_temp,
        t.reading_timestamp,
        t.seq_num,
        UNIFORM(1, 1000, RANDOM()) AS random_spike_check,
        UNIFORM(0, 1000, RANDOM()) AS temp_variation,
        UNIFORM(8, 12, RANDOM()) AS spike_magnitude,
        UNIFORM(1, 5, RANDOM()) AS sensor_num
    FROM customer_data c
    CROSS JOIN time_series t
)
SELECT
    customer_id,
    customer_name,
    reading_timestamp,
    CASE 
        WHEN random_spike_check <= 15 THEN 
            min_temp + (max_temp - min_temp) / 2 + spike_magnitude
        ELSE 
            min_temp + temp_variation * (max_temp - min_temp) / 1000.0
    END AS temperature_celsius,
    CONCAT('SENSOR-', customer_id, '-', LPAD(sensor_num::STRING, 3, '0')) AS sensor_id
FROM cross_joined
ORDER BY customer_id, reading_timestamp;

-- Create Semantic View for Cortex Analyst
CREATE SEMANTIC VIEW IF NOT EXISTS IOT_AGENT_DEMO.SENSOR_READINGS_SV
TABLES (
    readings AS IOT_AGENT_DEMO.SENSOR_READINGS
    PRIMARY KEY (customer_id, sensor_id, reading_timestamp)
    WITH SYNONYMS ('temperature readings', 'sensor data', 'IoT data')
    COMMENT = 'IoT sensor temperature readings from various customer facilities'
)
FACTS (
    PUBLIC readings.temperature_celsius AS temperature_celsius
    WITH SYNONYMS ('temperature', 'temp', 'celsius', 'degrees')
    COMMENT = 'Temperature measurement in degrees Celsius'
)
DIMENSIONS (
    PUBLIC readings.customer_id AS customer_id
    WITH SYNONYMS ('customer identifier', 'customer code', 'client id')
    COMMENT = 'Unique identifier for each customer',
    
    PUBLIC readings.customer_name AS customer_name
    WITH SYNONYMS ('customer', 'client', 'facility name', 'site')
    COMMENT = 'Name of the customer facility being monitored',
    
    PUBLIC readings.sensor_id AS sensor_id
    WITH SYNONYMS ('sensor identifier', 'device id', 'sensor name')
    COMMENT = 'Unique identifier for each temperature sensor',
    
    PUBLIC readings.reading_timestamp AS reading_timestamp
    WITH SYNONYMS ('time', 'timestamp', 'date', 'when', 'reading time', 'measurement time')
    COMMENT = 'Timestamp when the temperature reading was recorded'
)
METRICS (
    PUBLIC readings.avg_temperature AS AVG(temperature_celsius)
    WITH SYNONYMS ('average temperature', 'mean temperature', 'average temp', 'avg temp')
    COMMENT = 'Average temperature across selected readings',
    
    PUBLIC readings.min_temperature AS MIN(temperature_celsius)
    WITH SYNONYMS ('minimum temperature', 'lowest temperature', 'min temp')
    COMMENT = 'Minimum temperature recorded',
    
    PUBLIC readings.max_temperature AS MAX(temperature_celsius)
    WITH SYNONYMS ('maximum temperature', 'highest temperature', 'max temp', 'peak temperature')
    COMMENT = 'Maximum temperature recorded',
    
    PUBLIC readings.reading_count AS COUNT(*)
    WITH SYNONYMS ('number of readings', 'count of readings', 'total readings', 'data points')
    COMMENT = 'Total number of temperature readings'
)
COMMENT = 'Semantic view for IoT temperature sensor data analysis with Cortex Analyst';

-- create stage for benchmark pdfs
create stage if not exists customer_benchmarks
    DIRECTORY=(ENABLE=TRUE AUTO_REFRESH=TRUE)
    ENCRYPTION=(TYPE='SNOWFLAKE_SSE');

-- use snowsight or snowSQL to upload PDFs to stage
-- then confirm files exist
ls @customer_benchmarks;

-- Create table for PDF benchmark documents
CREATE TABLE IF NOT EXISTS IOT_AGENT_DEMO.CUSTOMER_BENCHMARK_DOCS AS
SELECT 
    RELATIVE_PATH as file_name,
    ai_parse_document(to_file('@customer_benchmarks', relative_path)) document_content,
    CASE 
        WHEN RELATIVE_PATH ILIKE '%Apex Cloud Data Center%' THEN 'CUST-DC-8472'
        WHEN RELATIVE_PATH ILIKE '%BioSyn Pharmaceutical%' THEN 'CUST-PH-3291'
        WHEN RELATIVE_PATH ILIKE '%Precision Automotive%' THEN 'CUST-AU-5614'
    END as customer_id,
    CASE 
        WHEN RELATIVE_PATH ILIKE '%Apex Cloud Data Center%' THEN 'Apex Cloud Data Center'
        WHEN RELATIVE_PATH ILIKE '%BioSyn Pharmaceutical%' THEN 'BioSyn Pharmaceutical Manufacturing'
        WHEN RELATIVE_PATH ILIKE '%Precision Automotive%' THEN 'Precision Automotive Components'
    END as customer_name
FROM DIRECTORY(@IOT_AGENT_DEMO.CUSTOMER_BENCHMARKS)
WHERE RELATIVE_PATH ILIKE '%.pdf';

-- Create Cortex Search Service for benchmark PDFs
CREATE CORTEX SEARCH SERVICE IF NOT EXISTS IOT_AGENT_DEMO.BENCHMARK_SEARCH_SERVICE
ON document_content
ATTRIBUTES customer_id, customer_name, file_name
WAREHOUSE = NLDEMO -- replace with your warehouse name
TARGET_LAG = '1 hour'
EMBEDDING_MODEL = 'snowflake-arctic-embed-m-v1.5'
COMMENT = 'Cortex Search Service for customer temperature benchmark specification PDFs'
AS (
    SELECT 
        document_content:content::varchar as document_content,
        customer_id,
        customer_name,
        file_name
    FROM IOT_AGENT_DEMO.CUSTOMER_BENCHMARK_DOCS
);

CREATE AGENT IF NOT EXISTS IOT_AGENT_DEMO.TEMPERATURE_MONITORING_AGENT
COMMENT = 'Agent for analyzing IoT temperature data against customer benchmarks'
FROM SPECIFICATION
$$
models:
  orchestration: claude-4-sonnet

orchestration:
  budget:
    seconds: 60
    tokens: 32000

instructions:
  response: "Provide clear analysis of temperature data compared to benchmarks. Include specific time periods and temperature values when relevant."
  orchestration: "First use BenchmarkSearch to retrieve customer temperature specifications, then use SensorAnalytics to query actual temperature readings for comparison."
  system: "You analyze IoT temperature sensor data against customer-specific benchmark specifications to identify compliance periods and temperature excursions."
  sample_questions:
    - question: "Were the temperatures at Apex Cloud Data Center within the expected range last week?"
      answer: "I'll check the benchmark specifications for Apex Cloud Data Center and compare them against the actual sensor readings from last week."
    - question: "Show me when BioSyn Pharmaceutical had temperature readings outside their acceptable range"
      answer: "I'll retrieve BioSyn's temperature benchmarks and analyze their sensor data to identify out-of-range periods."

tools:
  - tool_spec:
      type: "cortex_search"
      name: "BenchmarkSearch"
      description: "Searches customer temperature benchmark specifications to find expected temperature ranges and requirements for each facility"
  - tool_spec:
      type: "cortex_analyst_text_to_sql"
      name: "SensorAnalytics"
      description: "Queries IoT sensor temperature readings to analyze actual temperatures by customer, time period, and sensor"

tool_resources:
  BenchmarkSearch:
    search_service: "NLDEMO.IOT_AGENT_DEMO.BENCHMARK_SEARCH_SERVICE"
    id_column: "FILE_NAME"
    title_column: "CUSTOMER_NAME"
    max_results: 4
  SensorAnalytics:
    semantic_view: "NLDEMO.IOT_AGENT_DEMO.SENSOR_READINGS_SV"
    execution_environment:
      type: "warehouse"
      warehouse: ""
$$;