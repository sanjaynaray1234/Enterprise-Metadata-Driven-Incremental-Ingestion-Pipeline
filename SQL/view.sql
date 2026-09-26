-- Presentation-layer view: decouples ADF from the raw table schema
CREATE VIEW Customer_Transactions_View AS
SELECT
    TransactionID,
    CustomerName,
    PolicyType,
    PremiumAmount,
    LastModifiedDate
FROM Customer_Transactions;
