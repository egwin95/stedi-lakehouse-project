CREATE EXTERNAL TABLE customer_landing (
    customerName STRING,
    email STRING,
    phone STRING,
    birthDay STRING,
    serialNumber STRING,
    registrationDate BIGINT,
    lastUpdateDate BIGINT,
    shareWithResearchAsOfDate BIGINT,
    shareWithPublicAsOfDate BIGINT,
    shareWithFriendsAsOfDate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lakehouse-earlgwin-090306607434-us-east-1-an/customer_landing/';