ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

USE bessmertnyjde;

SELECT 
    content.kktRegId,
    content.userInn,
    subtype,
    content.totalSum,
    from_unixtime(
        CAST(regexp_extract(content.dateTime, '(\\d{13})', 1) AS BIGINT) DIV 1000
    ) AS dateTime
FROM kkt_transactions
LIMIT 50;
