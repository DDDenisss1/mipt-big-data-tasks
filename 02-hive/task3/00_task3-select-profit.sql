SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE bessmertnyjde;

SELECT 
    userinn, 
    day_of_month, 
    PRINTF('%.1f', CAST(profit AS DOUBLE)) AS profit
FROM (
    SELECT 
        userinn,
        day_of_month,
        profit,
        ROW_NUMBER() OVER (PARTITION BY userinn ORDER BY profit DESC, day_of_month ASC) AS rn
    FROM (
        SELECT 
            userinn,
            CAST(FROM_UNIXTIME(
                CAST(REGEXP_EXTRACT(datetime, '(\\d{13})', 1) AS BIGINT) DIV 1000, 
                'dd'
            ) AS INT) AS day_of_month,
            SUM(COALESCE(totalsum, 0)) AS profit
        FROM kkt_transactions_parquet
        WHERE datetime IS NOT NULL
        AND userinn IS NOT NULL
        GROUP BY userinn, CAST(FROM_UNIXTIME(
            CAST(REGEXP_EXTRACT(datetime, '(\\d{13})', 1) AS BIGINT) DIV 1000, 
            'dd'
        ) AS INT)
    ) daily_profit
) ranked
WHERE rn = 1
ORDER BY userinn ASC;
