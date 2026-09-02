ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE bessmertnyjde;

DROP TABLE IF EXISTS kkt_transactions;

CREATE EXTERNAL TABLE kkt_transactions (
    `_id` STRING,
    fsId STRING,
    subtype STRING,
    receiveDate STRING,
    protocolVersion INT,
    ofdId STRING,
    protocolSubversion INT,
    documentId BIGINT,
    content STRUCT<
        receiptCode: INT,
        bsoCode: INT,
        user: STRING,
        userInn: STRING,
        requestNumber: BIGINT,
        dateTime: STRING,
        shiftNumber: BIGINT,
        operationType: INT,
        taxationType: INT,
        operator: STRING,
        kktRegId: STRING,
        fiscalDriveNumber: STRING,
        retailPlaceAddress: STRING,
        buyerAddress: STRING,
        senderAddress: STRING,
        addressToCheckFiscalSign: STRING,
        nds18: BIGINT,
        nds10: BIGINT,
        nds0: BIGINT,
        ndsNo: BIGINT,
        ndsCalculated18: BIGINT,
        ndsCalculated10: BIGINT,
        totalSum: BIGINT,
        cashTotalSum: BIGINT,
        ecashTotalSum: BIGINT,
        fiscalDocumentNumber: BIGINT,
        fiscalSign: BIGINT,
        rawData: STRING,
        items: STRING,
        stornoItems: STRING,
        modifiers: STRING
    >
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true'
)
STORED AS TEXTFILE
LOCATION '/data/hive/fns2';
