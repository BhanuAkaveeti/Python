import pyodbc
import json
import time
import requests
import urllib
import os
from requests.auth import HTTPBasicAuth
import warnings
from utilities import compare
from multiprocessing import Pool
warnings.filterwarnings("ignore")
beginTime = time.time()

matchFile = r"<path>\matchfilename.txt";
unMatchFile = r"<path>\unmatchfilename.txt";
if os.path.exists(matchFile):
    os.remove(matchFile)
if os.path.exists(unMatchFile):
    os.remove(unMatchFile)

try:
    key='compareKey';
    recordsProcessed = 0;
    rowLimit = 50000;

    SOURCE_DSN_NAME = '<source DSN Name>'
    SOURCE_DATA_BASE_NAME = '<source Data Base name>'
    SOURCE_TABLE_NAME = '<source Table name>'
    SOURCE_TABLE_SCHEMA = '<source schema name>'

    TARGET_DSN_NAME = '<Target DSN Name>'
    TARGET_DATA_BASE_NAME = '<Target Database name>'
    TARGET_TABLE_NAME = '<Target table name>'
    TARGET_TABLE_SCHEMA = '<Target schema name>'

    columnConnection = pyodbc.connect("DSN=" + SOURCE_DSN_NAME,autocommit=True)
    columnCursor = columnConnection.cursor()
    columnQuery="""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
    """

    columnCursor.execute(columnQuery,(SOURCE_TABLE_NAME,SOURCE_TABLE_SCHEMA))
    columnList = []
    while True:
        columnResult = columnCursor.fetchall()
        if not columnResult:
            break
        for index, column in enumerate(columnResult):
            columnList.append(column[0])

    columnCursor.close()

    keyConnection = pyodbc.connect("DSN=" + SOURCE_DSN_NAME,autocommit=True)
    keyCursor = keyConnection.cursor()
    keyQuery="""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
    INNER JOIN
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
    ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
    AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
    AND KU.TABLE_NAME = ?
    AND KU.TABLE_SCHEMA = ?
    AND KU.TABLE_CATALOG = ?
    """

    keyResult=keyCursor.execute(keyQuery,(SOURCE_TABLE_NAME,SOURCE_TABLE_SCHEMA,SOURCE_DATA_BASE_NAME))
    keyList = []
    for keyIndex, keyRow in enumerate(keyResult):
        keyList.append(keyRow[0])

    keyCursor.close()

    sourceRowQueryString = 'SELECT * FROM ' + SOURCE_DATA_BASE_NAME + '.' + SOURCE_TABLE_SCHEMA + '.' + SOURCE_TABLE_NAME
    sourceRowConnection = pyodbc.connect("DSN=" + SOURCE_DSN_NAME,autocommit=True)
    sourceRowCursor = sourceRowConnection.cursor()

    targetRowQueryString = 'SELECT * FROM ' + TARGET_DATA_BASE_NAME + '.' + TARGET_TABLE_SCHEMA + '.' + TARGET_TABLE_NAME
    targetRowConnection = pyodbc.connect("DSN=" + TARGET_DSN_NAME,autocommit=True)
    targetRowCursor = targetRowConnection.cursor()

    sourceRowCursor.execute(sourceRowQueryString)
    targetRowCursor.execute(sourceRowQueryString)

    while True:
        batchTime = time.time()
        sourceRowResult = sourceRowCursor.fetchmany(rowLimit)
        if not sourceRowResult:
            break
        targetRowResult = targetRowCursor.fetchmany(rowLimit)
        if not targetRowResult:
            break

        sourceRowObjectList = []
        
        for i, sourceRow in enumerate(sourceRowResult):
            sourceKeyString = '';
            sourceRowObject = {}
            for j, sourceRowValue in enumerate(sourceRow):
                sourceRowObject[columnList[j]] = sourceRowValue
                if columnList[j] in keyList:
                    sourceKeyString = sourceKeyString + str(sourceRowValue)
            sourceRowObject["compareKey"] = sourceKeyString
            sourceRowObjectList.append(sourceRowObject)

        targetRowObjectList = []
        for m, targetRow in enumerate(targetRowResult):
            targetKeyString = '';
            targetRowObject = {}
            for n,targetRowValue in enumerate(targetRow):
                targetRowObject[columnList[n]] = targetRowValue
                if columnList[n] in keyList:
                    targetKeyString = targetKeyString + str(targetRowValue)
            targetRowObject["compareKey"] = targetKeyString
            targetRowObjectList.append(targetRowObject)

        compare(sourceRowObjectList, targetRowObjectList,matchFile,unMatchFile,key)
        recordsProcessed = recordsProcessed + len(targetRowObjectList)
        print(str(recordsProcessed) + ' rows compared')
        print ('Batch compare time for ' + str(len(targetRowObjectList)) + ' rows: ' + str(time.time() - batchTime));

    sourceRowCursor.close()
    targetRowCursor.close()
    print ('Total Time:' + str(time.time() - beginTime));

except Exception as e:
    print(e)
