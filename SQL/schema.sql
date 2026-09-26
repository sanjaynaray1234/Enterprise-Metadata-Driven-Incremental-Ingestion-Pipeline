-- Source table: transactional data being ingested
CREATE TABLE Customer_Transactions (
    TransactionID    INT             NOT NULL PRIMARY KEY,
    CustomerName     VARCHAR(100)    NULL,
    PolicyType       VARCHAR(50)     NULL,
    PremiumAmount    DECIMAL(10,2)   NULL,
    LastModifiedDate DATETIME2       NULL
);

-- Control table: tracks the last processed watermark per source table
CREATE TABLE WatermarkTable (
    TableName      VARCHAR(100)  NOT NULL PRIMARY KEY,
    WatermarkValue DATETIME2     NOT NULL
);

-- Seed row so the first pipeline run has a starting boundary
INSERT INTO WatermarkTable (TableName, WatermarkValue)
VALUES ('Customer_Transactions', '1900-01-01T00:00:00');
