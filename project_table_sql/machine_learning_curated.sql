CREATE EXTERNAL TABLE `machine_learning_curated`(
  `user` string COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer', 
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://thaovi-lake-house/machine-learning/curated/'
TBLPROPERTIES (
  'CreatedByJob'='Machine Learning Trusted To Curated', 
  'CreatedByJobRun'='jr_ed74adf3ad9fcebfdb72d328dcf20f851ce20175438d2f8d158becee215bfb63', 
  'classification'='json')