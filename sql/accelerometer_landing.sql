CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing (
    user STRING,
    timeStamp BIGINT,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)

ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-lakehouse-earlgwin-090306607434-us-east-1-an/accelerometer_landing/'