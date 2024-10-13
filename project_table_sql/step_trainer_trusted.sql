
CREATE EXTERNAL TABLE `step_trainer_trusted`(
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
  's3://thaovi-lake-house/step_trainer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Step Trainer Landing To Trusted', 
  'CreatedByJobRun'='jr_bd52826f2315e4c12504250e822dcd9fb364cc18fedc14aa558c35b22f931e99', 
  'classification'='json')