USE [HNB_Zoot_Prod]
GO
/****** Object:  StoredProcedure [dbo].[SP_DecisionReasonSummary_Consumer]    Script Date: 08/18/2025 06:01:47 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE .[dbo].[SP_DecisionReasonSummary_Consumer]
AS
IF EXISTS (SELECT 1
	         FROM HNB_Analysis.INFORMATION_SCHEMA.Tables
	        WHERE Table_Name = 'Raw_DECISION_REASON_AllPeriods_Consumer')
BEGIN 
    DROP TABLE HNB_Analysis.dbo.Raw_DECISION_REASON_AllPeriods_Consumer
END 

SELECT 
SUBSTRING(TABLE_NAME,10,8) AS PeriodId 
INTO HNB_Analysis.dbo.Raw_DECISION_REASON_AllPeriods_Consumer
--SELECT *
FROM HNB_Zoot_Raw.INFORMATION_SCHEMA.TABLES  
WHERE Table_Name LIKE 'Raw_Zoot__________DECISION' 
AND SUBSTRING(TABLE_NAME,10,8) 
NOT IN (SELECT SUBSTRING(TABLE_NAME,10,8) 
		FROM HNB_Zoot_Prod.INFORMATION_SCHEMA.TABLES  
		where Table_Name LIKE '%Raw_Zoot__________DECISION_REASON_Summary_Consumer'
		and Table_Name <> 'Raw_Zoot_DECISION_REASON_Summary_Consumer'
		)
AND 	CAST(SUBSTRING(TABLE_NAME,10,8)	AS DATE) > = '20171210'
ORDER BY 1

IF EXISTS (SELECT 1
	         FROM HNB_Analysis.INFORMATION_SCHEMA.Tables
	        WHERE Table_Name = 'HNB_PackagesLog_DecisionReasonSummaryTables_Consumer')
BEGIN 
    DROP TABLE HNB_Analysis.dbo.HNB_PackagesLog_DecisionReasonSummaryTables_Consumer
END 

select * 
		,CAST(NULL AS DATETIME) AS StartTime 
		,CAST(NULL AS DATETIME) AS EndTime 
		,CAST(NULL AS VARCHAR(100)) AS Status 
		,cast(NULL as datetime)LastUpdateDate
		,cast(NULL as varchar(50))LastUpdateLonId
INTO HNB_Analysis.dbo.HNB_PackagesLog_DecisionReasonSummaryTables_Consumer
FROM HNB_Analysis.dbo.Raw_DECISION_REASON_AllPeriods_Consumer



CREATE UNIQUE CLUSTERED INDEX Id_HNB_PackagesLog_DecisionReasonSummaryTables_Consumer
ON HNB_Analysis.dbo.HNB_PackagesLog_DecisionReasonSummaryTables_Consumer (PeriodId)
 


 
DECLARE Cursor_CreateDecisionReasonTable CURSOR
FOR 
 
SELECT
PeriodId
from HNB_Analysis.dbo.HNB_PackagesLog_DecisionReasonSummaryTables_Consumer
where status is null
and periodId >= 20171210
ORDER BY PeriodId DESC





DECLARE @PeriodId varchar(8), @BankId varchar(3), @VarSQL varchar(8000)
SET @VarSQL = ''
OPEN Cursor_CreateDecisionReasonTable
FETCH NEXT FROM Cursor_CreateDecisionReasonTable INTO @PeriodId
WHILE (@@fetch_status <> -1)
BEGIN
	IF (@@fetch_status <> -2)
	BEGIN
			--print @BankId
			SET @VarSQL = 
'

IF EXISTS (SELECT 1
	         FROM HNB_Zoot_Prod.INFORMATION_SCHEMA.Tables
	        WHERE Table_Name = ''Raw_Zoot_'+@PeriodId+'_DECISION_REASON_SUMMARY_Consumer'')
BEGIN 
    DROP TABLE HNB_Zoot_Prod.dbo.Raw_Zoot_'+@PeriodId+'_DECISION_REASON_SUMMARY_Consumer
END 

SELECT 
cast(case when App.Sub_Product_id in (3,5,6,8,9,16) then ''NON CLI''
			when App.Sub_product_id = 10 then ''CLI'' END as varchar) as CLIAPP
,DECISION.DECISION_ID
,DECISION_REASON.DECISION_REASON_ID
,DECISION.App_ID
,DECISION.DECISION_TYPE_CD
,LKP_DECISION_TYPE.LONG_DESC AS DECISION_TYPE_LONG_DESC
,Consumer.CONSUMER_ID
,DECISION_REASON.DECISION_REASON_TYPE_CD
,LKP_DECISION_REASON_TYPE.LONG_DESC AS DECISION_REASON_TYPE_LONG_DESC
,DECISION_REASON.OVERRIDE_REASON_TYPE_CD
,LKP_OVERRIDE_REASON_TYPE.LONG_DESC AS OVERRIDE_REASON_TYPE_LONG_DESC
,DECISION_REASON.COMMENTS
,DECISION.DECISION_STATUS_TYPE_CD
,LKP_DECISION_STATUS_TYPE.LONG_DESC AS DECISION_STATUS_TYPE_LONG_DESC 
,DECISION.CURRENT_PREFERRED_FLAG
,DECISION.DECISION_ORDER
,DECISION.INSRT_DTTM
,DECISION.INSRT_USR_ID
,DECISION.CHNG_DTTM
,DECISION.CHNG_USR_ID
,DECISION.ACTV_FLAG
--,DECISION.CORRELATION_ID

--,DECISION_USES_CREDIT_RPT.DECISION_USES_CREDIT_RPT_ID
--,DECISION_USES_CREDIT_RPT.CREDIT_RPT_ID

			
INTO HNB_Zoot_Prod.dbo.Raw_Zoot_'+@PeriodId+'_DECISION_REASON_SUMMARY_Consumer    
FROM HNB_Zoot_Raw.dbo.Raw_Zoot_'+@PeriodId+'_DECISION DECISION

LEFT JOIN HNB_Zoot_Raw.dbo.Raw_Zoot_'+@PeriodId+'_DECISION_TYPE LKP_DECISION_TYPE
ON  DECISION.DECISION_TYPE_CD=LKP_DECISION_TYPE.DECISION_TYPE_CD


LEFT JOIN HNB_Zoot_Raw.dbo.Raw_Zoot_'+@PeriodId+'_DECISION_STATUS_TYPE LKP_DECISION_STATUS_TYPE
ON  DECISION.DECISION_STATUS_TYPE_CD=LKP_DECISION_STATUS_TYPE.DECISION_STATUS_TYPE_CD

LEFT JOIN HNB_Zoot_Raw.dbo.Raw_Zoot_'+@PeriodId+'_DECISION_REASON DECISION_REASON
ON  DECISION.DECISION_ID=DECISION_REASON.DECISION_ID

LEFT JOIN HNB_Zoot_Raw.dbo.Raw_Zoot_'+@PeriodId+'_Consumer Consumer
ON  DECISION.APP_ID=Consumer.APP_ID

LEFT JOIN HNB_Zoot_Raw.dbo.Raw_Zoot_'+@PeriodId+'_DECISION_REASON_TYPE LKP_DECISION_REASON_TYPE
ON  DECISION_REASON.DECISION_REASON_TYPE_CD=LKP_DECISION_REASON_TYPE.DECISION_REASON_TYPE_CD


LEFT JOIN HNB_Zoot_Raw.dbo.Raw_Zoot_'+@PeriodId+'_OVERRIDE_REASON_TYPE LKP_OVERRIDE_REASON_TYPE
ON  DECISION_REASON.OVERRIDE_REASON_TYPE_CD=LKP_OVERRIDE_REASON_TYPE.OVERRIDE_REASON_TYPE_CD


Left join HNB_ZOOT_RAW.dbo.RAW_Zoot_'+@PeriodId+'_App App
on DECISION.App_id = App.App_id

where App.Product_id = 1
and consumer.Consumer_Type_cd = 1


--LEFT JOIN HNB_Zoot_Raw.dbo.Raw_Zoot_'+@PeriodId+'_DECISION_USES_CREDIT_RPT DECISION_USES_CREDIT_RPT
--ON  DECISION.DECISION_ID=DECISION_USES_CREDIT_RPT.DECISION_ID

UPDATE HNB_Analysis.dbo.HNB_PackagesLog_DecisionReasonSummaryTables_Consumer
SET 
	EndTime = getdate()
	,Status = ''Successfully finished''
	,LastUpdateDate = getdate()
	,LastUpdateLonId = user 
WHERE PeriodId = '+@PeriodId+'
'

EXEC (@VarSQL)

END
	
	FETCH NEXT FROM  Cursor_CreateDecisionReasonTable INTO @PeriodId
END

CLOSE Cursor_CreateDecisionReasonTable
DEALLOCATE Cursor_CreateDecisionReasonTable


IF EXISTS (SELECT 1
	         FROM HNB_Analysis.INFORMATION_SCHEMA.Tables
	        WHERE Table_Name = 'Decision_Reason_PeriodIDLookup_Consumer')
BEGIN 
    DROP TABLE HNB_Analysis.dbo.Decision_Reason_PeriodIDLookup_Consumer
END 


        SELECT SUBSTRING(TABLE_NAME,10,8) as PeriodID
	    INTO HNB_Analysis.dbo.Decision_Reason_PeriodIDLookup_Consumer
		FROM HNB_Zoot_Prod.INFORMATION_SCHEMA.TABLES  
		where Table_Name LIKE 'Raw_Zoot__________Decision_Reason_Summary_Consumer'
		AND Table_Name <> 'Raw_Zoot_Decision_Reason_Summary_Consumer'
        AND 	CAST(SUBSTRING(TABLE_NAME,10,8)	AS DATE) > = '20171210'
        ORDER BY 1
        
        



 
DECLARE Cursor_CreateDecisionReasonSummaryAllTable CURSOR
FOR 
 
SELECT DISTINCT a.PERIODID
FROM HNB_Analysis.dbo.Decision_Reason_PeriodIDLookup_Consumer a
left JOIN HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_ALL_Consumer b
on a.PeriodId=b.PeriodId
where b.PeriodID is NULL
 
ORDER BY 1

DECLARE @PeriodId1 varchar(8), @BankId1 varchar(3), @VarSQL1 varchar(8000)
SET @VarSQL1 = ''
OPEN Cursor_CreateDecisionReasonSummaryAllTable
FETCH NEXT FROM Cursor_CreateDecisionReasonSummaryAllTable INTO @PeriodId1
WHILE (@@fetch_status <> -1)
BEGIN
	IF (@@fetch_status <> -2)
	BEGIN
			--print @BankId
			SET @VarSQL1 = 'INSERT INTO HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_ALL_Consumer
							SELECT  '+@PeriodId1+' as PeriodID,*
							FROM HNB_Zoot_Prod.dbo.Raw_Zoot_'+@PeriodId1+'_Decision_REASON_SUMMARY_Consumer
							
							INSERT INTO HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_ALL_Consumer_Temp
							SELECT  '+@PeriodId1+' as PeriodID,*
							FROM HNB_Zoot_Prod.dbo.Raw_Zoot_'+@PeriodId1+'_Decision_REASON_SUMMARY_Consumer
							
							'
	END
	EXEC (@VarSQL1)
	FETCH NEXT FROM  Cursor_CreateDecisionReasonSummaryAllTable INTO @PeriodId1
END

CLOSE Cursor_CreateDecisionReasonSummaryAllTable
DEALLOCATE Cursor_CreateDecisionReasonSummaryAllTable




IF EXISTS (SELECT 1
	         FROM HNB_Analysis.INFORMATION_SCHEMA.Tables
	        WHERE Table_Name = 'Raw_Zoot_Decision_REASON_SUMMARY_ALL_WITHMOSTRECENT_UPDATEDATE_Consumer')
BEGIN 
    DROP TABLE HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_ALL_WITHMOSTRECENT_UPDATEDATE_Consumer
END 

SELECT DECISION_ID,MAX(CAST(CHNG_DTTM AS DATE)) AS CHNG_DTTM
INTO HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_ALL_WITHMOSTRECENT_UPDATEDATE_Consumer
FROM HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_ALL_Consumer_Temp
GROUP BY DECISION_ID


IF EXISTS (SELECT 1
	         FROM HNB_Analysis.INFORMATION_SCHEMA.Tables
	        WHERE Table_Name = 'Raw_Zoot_Decision_REASON_SUMMARY_TEMP_Consumer')
BEGIN 
    DROP TABLE HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP_Consumer
END 

 SELECT DISTINCT a.* 
 INTO HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP_Consumer
 FROM HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_ALL_Consumer_Temp a
 JOIN HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_ALL_WITHMOSTRECENT_UPDATEDATE_Consumer b
 ON a.DECISION_ID  = b.DECISION_ID
 AND a.CHNG_DTTM = b.CHNG_DTTM
 
 
 IF EXISTS (SELECT 1
	         FROM HNB_Analysis.INFORMATION_SCHEMA.Tables
	        WHERE Table_Name = 'Raw_Zoot_Decision_REASON_SUMMARY_TEMP1_Consumer')
BEGIN 
    DROP TABLE HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP1_Consumer
END 



Select DECISION_ID,DECISION_REASON_ID
into HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP1_Consumer
from HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP_Consumer
group by DECISION_ID,DECISION_REASON_ID
having count(*) >1


 IF EXISTS (SELECT 1
	         FROM HNB_Analysis.INFORMATION_SCHEMA.Tables
	        WHERE Table_Name = 'Raw_Zoot_Decision_REASON_SUMMARY_TEMP2_Consumer')
BEGIN 
    DROP TABLE HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP2_Consumer
END 

select *
into HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP2_Consumer
from HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP_Consumer
where DECISION_ID IN (select DECISION_ID from HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP1_Consumer)
and DECISION_REASON_ID IN (select DECISION_REASON_ID from HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP1_Consumer)





delete from HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP_Consumer
where DECISION_ID IN (select DECISION_ID from HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP1_Consumer)
and DECISION_REASON_ID IN (select DECISION_REASON_ID from HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP1_Consumer)



insert into HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP_Consumer
select *
from HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP2_Consumer
where override_reason_type_cd is not null
order by DECISION_ID,decision_Reason_ID



IF EXISTS (SELECT 1
	         FROM HNB_Analysis.INFORMATION_SCHEMA.Tables
	        WHERE Table_Name = 'Raw_Zoot_Decision_REASON_SUMMARY_Temp3_Consumer')
BEGIN 
    DROP TABLE HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_Temp3_Consumer
END 


select Distinct CLIAPP
,[DECISION_ID]
      ,[DECISION_REASON_ID]
      ,[App_ID]
      ,[DECISION_TYPE_CD]
      ,[DECISION_TYPE_LONG_DESC]
      ,[CONSUMER_ID]
      ,[DECISION_REASON_TYPE_CD]
      ,[DECISION_REASON_TYPE_LONG_DESC]
      ,[OVERRIDE_REASON_TYPE_CD]
      ,[OVERRIDE_REASON_TYPE_LONG_DESC]
      ,[COMMENTS]
      ,[DECISION_STATUS_TYPE_CD]
      ,[DECISION_STATUS_TYPE_LONG_DESC]
      ,[CURRENT_PREFERRED_FLAG]
      ,[DECISION_ORDER]
      ,[INSRT_DTTM]
      ,[INSRT_USR_ID]
      ,[CHNG_DTTM]
      ,[CHNG_USR_ID]
      ,[ACTV_FLAG]
INTO HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_Temp3_Consumer
FROM HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP_Consumer



 IF NOT EXISTS(

  select Decision_ID,Decision_Reason_ID,COUNT(*)
   FROM HNB_ANAlysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_Temp3_Consumer
   group by Decision_ID,Decision_Reason_ID
   having COUNT(*)>1)
  --- AND Decision_ID NOT IN (select Decision_ID FROM HNB_ZOOT_PROD.dbo.Raw_Zoot_Decision_REASON_SUMMARY_DUPES)
  --- AND Decision_Reason_ID NOT IN (select Decision_Reason_ID FROM HNB_ZOOT_PROD.dbo.Raw_Zoot_Decision_REASON_SUMMARY_DUPES))
   
   BEGIN 
   
   
     delete a
      
      
    from  HNB_Zoot_Prod.dbo.Raw_Zoot_Decision_REASON_SUMMARY_Consumer  a
    left JOIN HNB_ANALYSIS.dbo.Raw_Zoot_Decision_REASON_SUMMARY_Temp3_Consumer b
    on a.DECISION_ID = b.DECISION_ID
    and isnull(a.DECISION_REASON_ID,0)= isnull(b.DECISION_REASON_ID,0)
    --and a.CHNG_DTTM=b.CHNG_DTTM
    where b.DECISION_ID is not NULL
  
   
    insert INTO HNB_Zoot_Prod.dbo.Raw_Zoot_Decision_REASON_SUMMARY_Consumer
    SELECT a.*
       from  HNB_ANALYSIS.dbo.Raw_Zoot_Decision_REASON_SUMMARY_Temp3_Consumer a
    left JOIN HNB_Zoot_Prod.dbo.Raw_Zoot_Decision_REASON_SUMMARY_Consumer  b
    on a.DECISION_ID = b.DECISION_ID
     and isnull(a.DECISION_REASON_ID,0)= isnull(b.DECISION_REASON_ID,0)
    --and a.CHNG_DTTM=b.CHNG_DTTM
    where b.DECISION_REASON_ID is NULL

END





 
 
Truncate table HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_ALL_Consumer_Temp

-----DROP TABLE HNB_Zoot_Raw.dbo.Raw_Zoot_Decision_REASON_SUMMARY_ALL
DROP TABLE HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP_consumer
DROP TABLE HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP1_consumer
DROP TABLE HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP2_consumer
DROP TABLE HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_TEMP3_consumer
DROP TABLE HNB_Analysis.dbo.Raw_Zoot_Decision_REASON_SUMMARY_ALL_WITHMOSTRECENT_UPDATEDATE_consumer
GO
