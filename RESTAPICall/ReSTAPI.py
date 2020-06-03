# This program reads rows from a Source table (having columns FleetId and DriverId) and makes REST API calls by passing FleetId and DriverId as query parameters (one REST call
# for each row read from the source table), and inserts the JSON response into a Target table (one row for each REST API response). The JSON response is stored in a [nvarchar] datatype
# with length as (max). Below is the DDL for Target table.

# CREATE TABLE [dbo].[PRESTG_TELEMATICS_GPS_TRIP_SUMMARY](
#	[FleetId] [int] NULL,
#	[DriverId] [int] NULL,
#	[JSONResponse] [nvarchar](max) NULL,
#	[META_LastUpdateTimestamp] [datetime] NULL
# )

import mygeotab
import requests
import json
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
import sqlalchemy
import pyodbc
import time;
import math;
from datetime import timedelta
import requests
from requests.auth import HTTPBasicAuth

SourceDSNName = 'Source_DSN'            # Replace Source_DSN with actual DSN name of the SQL Server Database where the source table resides
SourceTableName = 'Source_TableName'    # Replace Source_TableName with actual table name from
SourceDB = 'Source_DatabaseName'        # Replace Source_DatabaseName with actual Database name where source table resides
SourceSchemaName = 'Source_SchemaName'  # Replace Source_SchemaName with actual schema name where source table resides

TargetDSNName = 'Target_DSN'            # Replace Target_DSN with actual DSN name of the SQL Server Database where the target table resides
TargetTableName = 'Target_TableName'    # Replace Target_TableName with actual table name where the response will be inserted
TargetDB = 'Target_TableName'           # Replace Target_DatabaseName with actual Database name where target table resides
TargetSchemaName = 'Target_SchemaName'  # Replace Target_SchemaName with actual schema name where target table resides

TimestampQueryString = 'SELECT CURRENT_TIMESTAMP'
Connection = pyodbc.connect("DSN=" + TargetDSNName,autocommit=True)
TimestampCursor = Connection.cursor()
TimestampCursor.execute(TimestampQueryString)
TimestampResult = TimestampCursor.fetchall()
for index, TimestampColumn in enumerate(TimestampResult):
    SessionTimestamp = TimestampColumn[0]

engine = sqlalchemy.create_engine("mssql+pyodbc://username:password@SourceDSNName") # Replace username, password, SourceDSNName with actual values

driverQuery = 'SELECT FLEETID, DRIVERID FROM ' + SourceDB + '.' + SourceSchemaName + '.' + SourceTableName
driverDF = pd.read_sql(driverQuery, con=engine)

for ind in driverDF.index:
    uri = 'baseuri' + '?fleetId=' + str(driverDF['FLEETID'][ind]) + '&driverId=' + str(driverDF['DRIVERID'][ind]) # Replace baseuri with actual value
    resp = requests.get(uri, auth=HTTPBasicAuth('username', 'password'))  # Replace username, password with actual values
    driverDF.at[ind, "FleetId"] = driverDF['FLEETID'][ind]
    driverDF.at[ind, "DriverId"] = driverDF['DRIVERID'][ind]
    driverDF.at[ind, "JSONResponse"] = str(resp.json())
    driverDF.at[ind, "META_LastUpdateTimestamp"] = SessionTimestamp

driverDF = driverDF.drop(columns=["FLEETID", "DRIVERID"])
driverDF.to_sql(TargetTableName, con=engine,schema=TargetSchemaName,if_exists='append',index=False)
