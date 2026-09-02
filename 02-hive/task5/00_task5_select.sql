SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE bessmertnyjde;

SELECT DISTINCT userinn
FROM (
    SELECT 
        userinn,
        kktregid,
        subtype,
        open_count,
        close_count
    FROM (
        SELECT 
            userinn,
            kktregid,
            subtype,
            ts,
            SUM(CASE WHEN subtype = 'openShift' THEN 1 ELSE 0 END) 
                OVER (PARTITION BY kktregid ORDER BY ts ROWS UNBOUNDED PRECEDING) AS open_count,
            SUM(CASE WHEN subtype = 'closeShift' THEN 1 ELSE 0 END) 
                OVER (PARTITION BY kktregid ORDER BY ts ROWS UNBOUNDED PRECEDING) AS close_count
        FROM (
            SELECT 
                userinn,
                kktregid,
                subtype,
                CAST(regexp_extract(datetime, '(\\d{13})', 1) AS BIGINT) DIV 1000 AS ts
            FROM kkt_transactions_parquet
            WHERE subtype IN ('receipt', 'openShift', 'closeShift')
        ) t
    ) t2
    WHERE subtype = 'receipt'
) violations
WHERE open_count = 0 OR close_count >= open_count
LIMIT 50;
