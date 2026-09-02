ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE bessmertnyjde;

DROP TABLE IF EXISTS kkt_transactions_text;

CREATE EXTERNAL TABLE kkt_transactions_text (
    subtype STRING,
    userInn STRING,
    totalSum BIGINT,
    dateTime STRING,
    kktRegId STRING
)
STORED AS TEXTFILE
LOCATION '/user/hobod2026s001/kkt_text';

INSERT OVERWRITE TABLE kkt_transactions_text
SELECT subtype, content.userInn, content.totalSum, content.dateTime, content.kktRegId
FROM kkt_transactions;
