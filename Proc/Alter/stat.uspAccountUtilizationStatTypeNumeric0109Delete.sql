USE [IFA]
GO
/****** Object:  StoredProcedure [stat].[uspAccountUtilizationStatTypeNumeric0109Delete]    Script Date: 8/15/2024 8:55:26 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****************************************************************************************
	Name: [stat].[uspAccountUtilizationStatTypeNumeric0109Delete]
	Created By: Larry Dugger
	Descr: Delete the records not in this batch
	History:
		2018-03-04 - LBD - Created, full recode
      2024-08-15 - LK  - only pulling statid's in (113,115,116,117), no longer looking to statgroupIDs
	  2024-10-15 - LK  - Changed [Condensed].[stat].[BatchLog] out for [Condensed].[stat].[IFABatchXref] correct date and updated column names for #tblStatTypeDelete
						 Lee added the xref table and said we had to switch out the tables for the correct dates.
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspAccountUtilizationStatTypeNumeric0109Delete](
	 @piPageSize INT = 1000
	,@pncDelay NCHAR(11) = '00:00:00.01'
)
AS
BEGIN
	SET NOCOUNT ON

----For testing
--DECLARE @piPageSize INT = 1000,
--        @pncDelay NCHAR(11) = '00:00:00.01'


-- Step 1: Persistent State Table
IF OBJECT_ID('dbo.BatchProcessState', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.BatchProcessState (
	Id INT IDENTITY(1,1) PRIMARY KEY,
	LastProcessedKeyElementId BIGINT,
	LastProcessedPartitionId TINYINT,
	LastProcessedStatId SMALLINT,
	LastProcessedDateTime DATETIME2(7)
    );
END

-- Step 2: Declare Variables
DECLARE @iLogLevel INT = 3,
	@iPageNumber INT = 1,
	@iPageCount INT = 1,
	@iPageSize INT = @piPageSize,
	@BatchSize INT = 100000, -- Number of records to process per batch
	@BatchNumber INT = 1, -- Initialize the batch number
	@ncDelay NCHAR(11) = @pncDelay,
	@bExists BIT = 0,
	@nvMessage NVARCHAR(512),
	@nvMessage2 NVARCHAR(512),
	@dt DATETIME2(7) = SYSUTCDATETIME(),
	@iStatGroupId INT,
	@dtRemoveBefore DATETIME2(7) = DATEADD(DAY,-180,SYSUTCDATETIME()),
	@iErrorDetailId INT,
	@sSchemaName NVARCHAR(128)= OBJECT_SCHEMA_NAME(@@PROCID),
	@LastProcessedKeyElementId BIGINT = NULL,
	@LastProcessedPartitionId TINYINT = NULL,
	@LastProcessedStatId SMALLINT = NULL;

-- Step 3: Create Temporary Tables
DROP TABLE IF EXISTS #tblAUBatchLogId
CREATE TABLE #tblAUBatchLogId(
    IFABatchId SMALLINT PRIMARY KEY,
    IFABatchVersionEffectDatetime INT
);

DROP TABLE IF EXISTS #tblStatTypeDelete
CREATE TABLE #tblStatTypeDelete(
    PartitionId TINYINT,
    KeyElementId BIGINT,
    StatId SMALLINT,
    PRIMARY KEY (KeyElementId, PartitionId, StatId)
);

-- Step 4: Load Last State if Exists
-- This section loads the last processed state from the BatchProcessState table to resume from the last point if the script was interrupted.
IF EXISTS (SELECT 1 FROM dbo.BatchProcessState)
BEGIN
PRINT 'Select from BatchProcessState'
    SELECT TOP 1 
	@LastProcessedKeyElementId = LastProcessedKeyElementId,
	@LastProcessedPartitionId = LastProcessedPartitionId,
	@LastProcessedStatId = LastProcessedStatId
    FROM dbo.BatchProcessState
    ORDER BY Id DESC;
END

-- Step 5: Log Start of Execution
	SET @nvMessage = N'Executing ' + CASE WHEN (ISNULL(OBJECT_NAME(@@PROCID), N'') = N'') THEN N'a script ( ' + QUOTENAME(HOST_NAME()) + N':' + QUOTENAME(SUSER_SNAME()) + N' SPID=' + CONVERT(NVARCHAR(50), @@SPID) + N' PROCID=' + CONVERT(NVARCHAR(50), @@PROCID) + N' )' 
	ELSE N'database object ' + QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + N'.' + QUOTENAME(OBJECT_NAME(@@PROCID)) 
	END + N' on ' + QUOTENAME(@@SERVERNAME) + N'.' + QUOTENAME(DB_NAME());

IF @iLogLevel > 0
BEGIN
    INSERT INTO 
	[Condensed].[dbo].[StatLog]([Message])
    SELECT @nvMessage;
END

/*
-- Step 6: Insert BatchLogs to Remove
	INSERT INTO #tblAUBatchLogId(BatchLogId, StatGroupId)
	SELECT BatchLogId, DateActivated
	FROM [Condensed].[stat].[BatchLog]
	WHERE DateActivated < @dtRemoveBefore
	order by DateActivated
*/

-- Step 6: Insert BatchLogs to Remove
	INSERT INTO #tblAUBatchLogId(IFABatchId, IFABatchVersionEffectDatetime)
	SELECT IFABatchId, IFABatchVersionEffectDatetime
	FROM [Condensed].[stat].[IFABatchXref]
	WHERE IFABatchVersionEffectDatetime < @dtRemoveBefore
	order by IFABatchVersionEffectDatetime


BEGIN TRY
    -- Step 7: Check if Any to Process
    SELECT @bExists = 1 FROM #tblAUBatchLogId

    -- Step 8: Insert Records to be Deleted
    INSERT INTO #tblStatTypeDelete(KeyElementId, PartitionId, StatId)
    SELECT KeyElementId, PartitionId, StatId
    FROM [IFA].[stat].[StatTypeNumeric0109] st
    INNER JOIN #tblAUBatchLogId bl ON st.BatchLogId = bl.IFABatchId
    WHERE @bExists = 1 AND StatId IN (113, 115, 116, 117)
	AND (KeyElementId > ISNULL(@LastProcessedKeyElementId, 0)
	OR (KeyElementId = @LastProcessedKeyElementId AND PartitionId > ISNULL(@LastProcessedPartitionId, 0))
	OR (KeyElementId = @LastProcessedKeyElementId AND PartitionId = @LastProcessedPartitionId AND StatId > ISNULL(@LastProcessedStatId, 0)))
    ORDER BY KeyElementId ASC, PartitionId ASC, StatId ASC;


    IF @iLogLevel > 2 AND @bExists = 1
    BEGIN
	SET @nvMessage2 = @nvMessage + ' Populate #tblStatTypeDelete Took ' + CONVERT(NVARCHAR(20), DATEDIFF(MICROSECOND, @dt, SYSUTCDATETIME())) + ' mcs';
	INSERT INTO [Condensed].[dbo].[StatLog]([Message])
	SELECT @nvMessage2;
	SET @dt = SYSUTCDATETIME();
    END

    -- Step 9: Calculate Page Count
    SELECT @iPageCount = CEILING(COUNT(1) / (@iPageSize * 1.0))
    FROM #tblStatTypeDelete src
    WHERE @bExists = 1
	Select @iPageCount, @iPageSize, @bExists


    IF @iLogLevel > 1 AND @bExists = 1
    BEGIN
	SET @nvMessage2 = @nvMessage + ' Delete PageCount: ' + CONVERT(NVARCHAR(20), @iPageCount) + ' Took ' + CONVERT(NVARCHAR(20), DATEDIFF(MICROSECOND, @dt, SYSUTCDATETIME())) + ' mcs';
	INSERT INTO [Condensed].[dbo].[StatLog]([Message])
	SELECT @nvMessage2;
	SET @dt = SYSUTCDATETIME();
    END

    -- Step 10: Delete in Pages and Batches with Transactions
    WHILE @bExists = 1 AND @iPageNumber <= @iPageCount
    BEGIN
	WHILE EXISTS (
	SELECT 1
	FROM #tblStatTypeDelete
	WHERE KeyElementId > ISNULL(@LastProcessedKeyElementId, 0)
	OR (KeyElementId = @LastProcessedKeyElementId AND PartitionId > ISNULL(@LastProcessedPartitionId, 0))
	OR (KeyElementId = @LastProcessedKeyElementId AND PartitionId = @LastProcessedPartitionId AND StatId > ISNULL(@LastProcessedStatId, 0))
)

BEGIN
            -- Begin a new transaction for each batch
Select @BatchSize
BEGIN TRANSACTION;
	BEGIN TRY
	DELETE dst
	FROM [IFA].[stat].[StatTypeNumeric0109] dst
	INNER JOIN (
	SELECT TOP (@BatchSize) PartitionId, KeyElementId, StatId
	FROM #tblStatTypeDelete 
	WHERE KeyElementId > ISNULL(@LastProcessedKeyElementId, 0)
	OR (KeyElementId = @LastProcessedKeyElementId AND PartitionId > ISNULL(@LastProcessedPartitionId, 0))
	OR (KeyElementId = @LastProcessedKeyElementId AND PartitionId = @LastProcessedPartitionId AND StatId > ISNULL(@LastProcessedStatId, 0))
	ORDER BY KeyElementId ASC, PartitionId ASC, StatId ASC
	) AS src ON dst.PartitionId = src.PartitionId AND dst.KeyElementId = src.KeyElementId AND dst.StatId = src.StatId;

	-- Update the state table after each batch
	-- This section updates the BatchProcessState table after processing each batch, ensuring that the script can resume from this point if interrupted.
IF @@ROWCOUNT > 0
	BEGIN
	DELETE FROM dbo.BatchProcessState;
	INSERT INTO dbo.BatchProcessState(LastProcessedKeyElementId, LastProcessedPartitionId, LastProcessedStatId, LastProcessedDateTime)
	SELECT TOP 1 src.KeyElementId, src.PartitionId, src.StatId, SYSUTCDATETIME()
	FROM #tblStatTypeDelete src
	ORDER BY src.KeyElementId DESC, src.PartitionId DESC, src.StatId DESC;
END
	-- Commit the transaction
COMMIT TRANSACTION;

	-- Logging After Each Batch
IF @iLogLevel > 2
	BEGIN
	SET @nvMessage2 = @nvMessage + ' Delete dst Batch: ' + CONVERT(NVARCHAR(20), @BatchNumber) + ' Took ' + CONVERT(NVARCHAR(20), DATEDIFF(MICROSECOND, @dt, SYSUTCDATETIME())) + ' mcs';
	INSERT INTO [Condensed].[dbo].[StatLog]([Message])
	SELECT @nvMessage2;
	SET @dt = SYSUTCDATETIME();
	END

	-- Increment the batch number and introduce a delay between batches
	Print 'BatchNumber Assignment'
	SET @BatchNumber += 1;
	WAITFOR DELAY @ncDelay;
	END TRY
		BEGIN CATCH

 -- Rollback the transaction in case of error
IF XACT_STATE() <> 0
	BEGIN
ROLLBACK TRANSACTION;
	END

	-- Log the error
	EXEC [IFA].[error].[uspLogErrorDetailInsertOut] @psSchemaName = @sSchemaName, @piErrorDetailId = @iErrorDetailId OUTPUT;

IF @iLogLevel > 0
	BEGIN
	SET @nvMessage = N'Errored ' + CASE WHEN (ISNULL(OBJECT_NAME(@@PROCID), N'') = N'') THEN N'a script ( ' + QUOTENAME(HOST_NAME()) + N':' + QUOTENAME(SUSER_SNAME()) + N' SPID=' + CONVERT(NVARCHAR(50), @@SPID) + N' PROCID=' + CONVERT(NVARCHAR(50), @@PROCID) + N' )' 
	ELSE N'database object ' + QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + N'.' + QUOTENAME(OBJECT_NAME(@@PROCID)) 
	END + N' on ' + QUOTENAME(@@SERVERNAME) + N'.' + QUOTENAME(DB_NAME()) + N' ErrorDetailId=' + CONVERT(NVARCHAR(20), @iErrorDetailId);
	INSERT INTO [Condensed].[dbo].[StatLog]([Message])
	SELECT @nvMessage;
	END

 -- Exit the procedure in case of error
	RETURN;
		END CATCH
				END

 -- Move to the next page
	SET @iPageNumber += 1;
	Select @iPageNumber
    END
END TRY
BEGIN CATCH

    -- Error Handling outside the main loop
    EXEC [IFA].[error].[uspLogErrorDetailInsertOut] @psSchemaName = @sSchemaName, @piErrorDetailId = @iErrorDetailId OUTPUT;

IF @iLogLevel > 0
    BEGIN
        SET @nvMessage = N'Errored ' + CASE WHEN (ISNULL(OBJECT_NAME(@@PROCID), N'') = N'') THEN N'a script ( ' + QUOTENAME(HOST_NAME()) + N':' + QUOTENAME(SUSER_SNAME()) + N' SPID=' + CONVERT(NVARCHAR(50), @@SPID) + N' PROCID=' + CONVERT(NVARCHAR(50), @@PROCID) + N' )' 
		ELSE N'database object ' + QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + N'.' + QUOTENAME(OBJECT_NAME(@@PROCID)) 
		END + N' on ' + QUOTENAME(@@SERVERNAME) + N'.' + QUOTENAME(DB_NAME()) + N' ErrorDetailId=' + CONVERT(NVARCHAR(20), @iErrorDetailId);
        INSERT INTO [Condensed].[dbo].[StatLog]([Message])
        SELECT @nvMessage;
    END

    -- Rollback the transaction in case of error
IF XACT_STATE() <> 0
    BEGIN
	ROLLBACK TRANSACTION;
    END

    RETURN;
END CATCH

 -- Final Logging
IF @iLogLevel > 0
BEGIN
    SET @nvMessage = N'Executed ' + CASE WHEN (ISNULL(OBJECT_NAME(@@PROCID), N'') = N'') THEN N'a script ( ' + QUOTENAME(HOST_NAME()) + N':' + QUOTENAME(SUSER_SNAME()) + N' SPID=' + CONVERT(NVARCHAR(50), @@SPID) + N' PROCID=' + CONVERT(NVARCHAR(50), @@PROCID) + N' )' 
	ELSE N'database object ' + QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + N'.' + QUOTENAME(OBJECT_NAME(@@PROCID)) 
	END + N' on ' + QUOTENAME(@@SERVERNAME) + N'.' + QUOTENAME(DB_NAME());
    INSERT INTO [Condensed].[dbo].[StatLog]([Message])
    SELECT @nvMessage;
END

END