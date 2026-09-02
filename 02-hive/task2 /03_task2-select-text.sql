ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE bessmertnyjde;

SELECT userInn, SUM(totalSum) AS profit
FROM kkt_transactions_text
WHERE subtype = 'receipt'
GROUP BY userInn
ORDER BY profit DESC
LIMIT 1;
