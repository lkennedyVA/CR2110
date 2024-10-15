USE [IFA]
GO

/****** Object:  StoredProcedure [stat].[uspAccountUtilizationStatTypeNumeric0109Delete]    Script Date: 10/15/2024 2:37:18 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/****************************************************************************************
	Name: [stat].[uspAccountUtilizationStatTypeNumeric0109Delete]
	Created By: Larry Dugger
	Descr: Delete the records not in this batch
			
	Tables: [stat].[StatTypeNumeric0109]
		,[dbo].[StatLog] --synonym pointing to Condensed
	History:
		2018-03-04 - LBD - Created, full recode
*****************************************************************************************/
ALTER PROCEDURE [stat].[uspAccountUtilizationStatTypeNumeric0109Delete](
	 @piPageSize INT = 1000
	,@pncDelay NCHAR(11) = '00:00:00.01'
)
AS
BEGIN
	SET NOCOUNT ON
	CREATE table #tblAUBatchLogId(
		BatchLogId smallint primary key
	);
	CREATE table #tblStatTypeDelete(
		 PartitionId tinyint
		,KeyElementId bigint
		,StatId smallint
		,PRIMARY KEY (KeyElementId,PartitionId,StatId)
	);
	DECLARE @iLogLevel int = 3
		,@iPageNumber int = 1
		,@iPageCount int = 1
		,@iPageSize int = @piPageSize
		,@ncDelay nchar(11) = @pncDelay
		,@bExists bit = 0
		,@nvMessage nvarchar(512)
		,@nvMessage2 nvarchar(512)
		,@dt datetime2(7) = sysdatetime()
		,@iStatGroupId int
		,@dtRemoveBefore datetime2(7) = DATEADD(DAY,-180,sysdatetime()) --Only those older than 180 days (6 months)
		,@iErrorDetailId INT
		,@sSchemaName nvarchar(128)= OBJECT_SCHEMA_NAME( @@PROCID );

	SELECT @nvMessage = N'Executing ' 
		+ CASE 
			WHEN ( ISNULL( OBJECT_NAME( @@PROCID ), N'' ) = N'' ) 
				THEN N'a script ( ' + QUOTENAME( HOST_NAME() ) + N':' + QUOTENAME( SUSER_SNAME() ) + N' SPID=' + CONVERT( nvarchar(50), @@SPID ) + N' PROCID=' + CONVERT( nvarchar(50), @@PROCID ) + N' )' 
			ELSE N'database object ' + QUOTENAME( OBJECT_SCHEMA_NAME( @@PROCID ) ) + N'.' + QUOTENAME( OBJECT_NAME( @@PROCID ) ) 
			END + N' on ' + QUOTENAME( @@SERVERNAME ) + N'.' + QUOTENAME( DB_NAME() )
		,@dt = sysdatetime()
	WHERE @iLogLevel > 0;
	INSERT INTO [dbo].[StatLog]([Message])
	SELECT @nvMessage
	WHERE @iLogLevel > 0;

	SELECT @iStatGroupId = StatGroupId
	FROM [stat].[StatGroup]
	WHERE [Name] = 'Account Utilization';
	--BATCHLOGS to remove
	INSERT INTO #tblAUBatchLogId(BatchLogId)
	SELECT BatchLogId		
	FROM [Condensed].[stat].[BatchLog]
		WHERE StatGroupId =  @iStatGroupId 
			AND DateActivated < @dtRemoveBefore;

	BEGIN TRY
		--Any to Process 
		SELECT @bExists = 1
		FROM #tblAUBatchLogId;

		INSERT INTO #tblStatTypeDelete(KeyElementId, PartitionId, StatId)
		SELECT KeyElementId, PartitionId, StatId
		FROM [stat].[StatTypeNumeric0109] st
		INNER JOIN #tblAUBatchLogId bl ON st.BatchLogId = bl.BatchLogId
		WHERE @bExists = 1
		ORDER BY st.[KeyElementId] ASC;

		SELECT @nvMessage2 = @nvMessage + ' Populate #tblStatTypeDelete Took '+convert(nvarchar(20),datediff(microsecond,@dt,sysdatetime()))+ ' mcs'
			,@dt = sysdatetime()
		WHERE @bExists = 1
			AND @iLogLevel > 2;
		INSERT INTO [dbo].[StatLog]([Message])
		SELECT @nvMessage2
		WHERE @bExists = 1
			AND @iLogLevel > 2;

		SELECT @iPageCount = CEILING((COUNT(1)/(@iPageSize*1.0))) --returns same integer, or +1 if fraction exists
		FROM #tblStatTypeDelete src
		WHERE @bExists = 1;

		SELECT @nvMessage2 = @nvMessage + ' Delete PageCount:'+ CONVERT(nvarchar(20),@iPageCount) +' Took '+convert(nvarchar(20),datediff(microsecond,@dt,sysdatetime()))+ ' mcs'
		WHERE @bExists = 1
			AND @iLogLevel > 1;
		INSERT INTO [dbo].[StatLog]([Message])
		SELECT @nvMessage2
		WHERE @bExists = 1
			AND @iLogLevel > 1;
		SET @dt = sysdatetime();

		--DELETE  only
		WHILE @bExists = 1
			AND @iPageNumber <= @iPageCount
		BEGIN
			DELETE dst
			FROM [stat].[StatTypeNumeric0109] dst
			INNER JOIN (SELECT PartitionId, KeyElementId, StatId
						FROM #tblStatTypeDelete 
						ORDER BY [KeyElementId] ASC
						OFFSET @iPageSize * (@iPageNumber -1) ROWS
						FETCH NEXT @iPageSize ROWS ONLY) AS src ON dst.PartitionId = src.PartitionId
																AND dst.KeyElementId = src.KeyElementId
																AND dst.StatId = src.StatId;

			SELECT @nvMessage2 = @nvMessage + ' Delete dst Page:'+convert(nvarchar(20),@iPageNumber)+ ' Took '+convert(nvarchar(20),datediff(microsecond,@dt,sysdatetime()))+ ' mcs'
			WHERE @iLogLevel > 2;
			INSERT INTO [dbo].[StatLog]([Message])
			SELECT @nvMessage2
			WHERE @iLogLevel > 2;
			SET @dt = sysdatetime();

			SET @iPageNumber += 1;

			WAITFOR DELAY @ncDelay;	
		END
	END TRY
	BEGIN CATCH
		EXEC [error].[uspLogErrorDetailInsertOut] @psSchemaName = @sSchemaName, @piErrorDetailId=@iErrorDetailId OUTPUT;
		SELECT @nvMessage = N'Errored ' 
		+ CASE 
			WHEN ( ISNULL( OBJECT_NAME( @@PROCID ), N'' ) = N'' ) 
				THEN N'a script ( ' + QUOTENAME( HOST_NAME() ) + N':' + QUOTENAME( SUSER_SNAME() ) + N' SPID=' + CONVERT( nvarchar(50), @@SPID ) + N' PROCID=' + CONVERT( nvarchar(50), @@PROCID ) + N' )' 
			ELSE N'database object ' + QUOTENAME( OBJECT_SCHEMA_NAME( @@PROCID ) ) + N'.' + QUOTENAME( OBJECT_NAME( @@PROCID ) ) 
			END + N' on ' + QUOTENAME( @@SERVERNAME ) + N'.' + QUOTENAME( DB_NAME() ) + N' ErrorDetailId=' +CONVERT(NVARCHAR(20),@iErrorDetailId)
		WHERE @iLogLevel > 0;
		INSERT INTO [dbo].[StatLog]([Message])
		SELECT @nvMessage
		WHERE @iLogLevel > 0;
		RETURN		
	END CATCH
	SELECT @nvMessage = N'Executed ' 
	+ CASE 
		WHEN ( ISNULL( OBJECT_NAME( @@PROCID ), N'' ) = N'' ) 
			THEN N'a script ( ' + QUOTENAME( HOST_NAME() ) + N':' + QUOTENAME( SUSER_SNAME() ) + N' SPID=' + CONVERT( nvarchar(50), @@SPID ) + N' PROCID=' + CONVERT( nvarchar(50), @@PROCID ) + N' )' 
		ELSE N'database object ' + QUOTENAME( OBJECT_SCHEMA_NAME( @@PROCID ) ) + N'.' + QUOTENAME( OBJECT_NAME( @@PROCID ) ) 
		END + N' on ' + QUOTENAME( @@SERVERNAME ) + N'.' + QUOTENAME( DB_NAME() )
	WHERE @iLogLevel > 0;
	INSERT INTO [dbo].[StatLog]([Message])
	SELECT @nvMessage
	WHERE @iLogLevel > 0;
END
GO


