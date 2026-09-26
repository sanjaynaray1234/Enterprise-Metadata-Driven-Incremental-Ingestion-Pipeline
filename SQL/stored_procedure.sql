-- Called by ADF's "Stored procedure1" activity after every successful copy,
-- to persist the new high-watermark for the next incremental run.
CREATE PROCEDURE usp_write_watermark
    @LastModifiedDate DATETIME,
    @TableName VARCHAR(100)
AS
BEGIN
    UPDATE WatermarkTable
    SET WatermarkValue = @LastModifiedDate
    WHERE TableName = @TableName;
END;
