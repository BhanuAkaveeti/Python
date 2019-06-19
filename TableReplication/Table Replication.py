import pandas as pd
import sqlalchemy
import pyodbc
import time;

ETLTableName = 'ETL_Load'

TableListDF = pd.read_csv('TableReplicationList.txt',sep="|")

for index, row in TableListDF.iterrows():
    SourceDatabaseName = row['SourceDatabaseName']
    SourceTableName = row['SourceTableName']
    SourceSchemaName = row['SourceSchemaName']
    SourceDSNName = row['SourceDSNName']
    SourceTimestampColumn = row['SourceTimestampColumn']
    TargetDatabaseName = row['TargetDatabaseName']
    TargetTableName = row['TargetTableName']
    TargetSchemaName = row['TargetSchemaName']
    TargetDSNName = row['TargetDSNName']

    SourceDB = sqlalchemy.create_engine("mssql+pyodbc://" + SourceDSNName)
    TargetDB = sqlalchemy.create_engine("mssql+pyodbc://" + TargetDSNName)

    TimestampQueryString = 'SELECT CURRENT_TIMESTAMP'
    SelectQueryString = 'SELECT * FROM ETL_Load WHERE ProcessName = ' + '\'' + TargetDatabaseName + '.' + TargetSchemaName + '.' + TargetTableName + '\''

    Connection = pyodbc.connect("DSN=" + TargetDSNName,autocommit=True)
    TimestampCursor = Connection.cursor()
    TimestampCursor.execute(TimestampQueryString)
    TimestampResult = TimestampCursor.fetchall()
    for index, TimestampColumn in enumerate(TimestampResult):
        SessionTimestamp = TimestampColumn[0]

    SelectCursor = Connection.cursor()

    SelectCursor.execute(SelectQueryString)

    SelectResult = SelectCursor.fetchall()
    if SelectResult:
        for index, column in enumerate(SelectResult):
            LastRunTstmp = column[1]
    else:
        LastRunTstmp = '1900-01-01 01:01:01.000'

    print('LastRunTstmp:' + str(LastRunTstmp))
    SourceQuery = 'SELECT * FROM ' + SourceDatabaseName + '.' + SourceSchemaName + '.' + SourceTableName + ' WHERE ' + SourceTimestampColumn + ' > ' + '\'' + str(LastRunTstmp) + '\''
    SourceDF = pd.read_sql(SourceQuery, SourceDB)
    SourceRecordCount = len(SourceDF.index)

    SourceDF.to_sql(TargetTableName, TargetDB,schema=TargetSchemaName,if_exists='replace',index=False)

    TargetQuery = 'SELECT * FROM ' + TargetDatabaseName + '.' + TargetSchemaName + '.' + TargetTableName
    TargetDF = pd.read_sql(TargetQuery, TargetDB)
    TargetRecordCount = len(TargetDF.index)

    UpdateQueryString = 'UPDATE ETL_Load SET LastRunTstmp = ' + '\'' + str(SessionTimestamp) + '\'' + \
    ', SourceRecordCount = ' + str(SourceRecordCount) + \
    ', TargetRecordCount = ' + str(TargetRecordCount) + \
    ', ModifiedBy = ' + '\'' + TargetTableName + '\'' + \
    ', ModifiedOn = ' + '\'' + str(SessionTimestamp) + '\'' + \
    ' WHERE ProcessName = ' + '\'' + TargetDatabaseName + '.' + TargetSchemaName + '.' + TargetTableName + '\''

    InsertQueryString = 'INSERT INTO ETL_Load values (' + \
    '\'' + TargetDatabaseName + '.' + TargetSchemaName + '.' + TargetTableName + '\'' + ',' + \
    '\'' + str(SessionTimestamp) + '\'' + ',' + \
    '\'' + str(SourceRecordCount) + '\'' + ',' + \
    '\'' + str(TargetRecordCount) + '\'' + ',' + \
    '\'' + str(TargetTableName) + '\'' + ',' + \
    '\'' + str(SessionTimestamp) + '\'' + ')'

    if SelectResult:
        UpsertQueryString = UpdateQueryString
    else:
        UpsertQueryString = InsertQueryString

    UpsertCursor = Connection.cursor()
    UpsertCursor.execute(UpsertQueryString)

    UpsertCursor.close()    
    SelectCursor.close

