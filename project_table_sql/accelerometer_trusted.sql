CREATE EXTERNAL TABLE `accelerometer_trusted`(
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://thaovi-lake-house/accelerometer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Accelerometer Landing To Trusted', 
  'CreatedByJobRun'='jr_b654880d797a77b92cd740cdb3ed98f1b8fd90f60216d6cbb53171665eda5987', 
  'classification'='json')