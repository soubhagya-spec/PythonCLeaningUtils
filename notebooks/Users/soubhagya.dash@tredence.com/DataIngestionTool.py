# Databricks notebook source
def int_check(int_str,column_name):
    dataqualitychkmsg = "VALID"
    if (int_str.find(".") == -1):
        try:
            float(int_str)
        except ValueError:
            dataqualitychkmsg =  column_name + " column is Not an Integer"
            return dataqualitychkmsg
        else:
            if(float(int_str).is_integer()):
                return dataqualitychkmsg
            else:
                dataqualitychkmsg =  column_name + " column is Not an Integer"
                return dataqualitychkmsg
    else:
        dataqualitychkmsg =  column_name + " column is Not an Integer"
        return dataqualitychkmsg
    
    
def double_check(double_str,column_name):
    dataqualitychkmsg = "VALID"
    try:
        float(double_str)
    except:
        dataqualitychkmsg =  column_name + " column is Not a Double"
    return dataqualitychkmsg    
    
    
def date_check(datetime_str,format_date,column_name):
    dataqualitychkmsg = "VALID"
    from datetime import datetime
    try:
        datetime_object = datetime.strptime(datetime_str, format_date).date()
        return dataqualitychkmsg
    except:
        dataqualitychkmsg =  column_name + " column has Date format mismatch"
        return dataqualitychkmsg  

def timestamp_check(datetime_str,format_timestamp,column_name):
    dataqualitychkmsg = "VALID"
    from datetime import datetime
    try:
        datetime_object = datetime.strptime(datetime_str, format_timestamp)
        return dataqualitychkmsg
    except:
        dataqualitychkmsg =  column_name + " column has Timestamp format mismatch"
        return dataqualitychkmsg  
    
def null_check (in_str,column_name):
    dataqualitychkmsg = "VALID"
    in_str = str(in_str)
    try:
        in_str = in_str.strip()
        if(in_str.strip().lower() == "null" or in_str.strip().lower() == "none" or in_str.strip().lower() == "nan" or in_str.strip().lower() == "null" or len(in_str.strip())==0 ):
            dataqualitychkmsg =  column_name + " column is Null"
            return dataqualitychkmsg 
        else:
            return dataqualitychkmsg 
    except:
        if(not in_str):
            dataqualitychkmsg =  column_name + " column is Null"
            return dataqualitychkmsg 
        else:
            return dataqualitychkmsg
    
def length_check(in_str,length,column_name):
    dataqualitychkmsg = "VALID"
    if(len(in_str.strip()) == 0 or len(in_str.strip()) != length ):
        dataqualitychkmsg =  column_name + " column length is not matching"
        return dataqualitychkmsg 
    else:
        return dataqualitychkmsg
      
      
def check_function(row_dict,metadata_dict):
    err_col = []
    err_message = []
    Flag = ''
    for key,value in row_dict.items():
        col_dict = metadata_dict[key]
        if(col_dict["Nullable"] == "No"):
            err_text = null_check(value,key)
            if (err_text != "VALID"):
                err_message.append(err_text)
                err_col.append(key)
                if (col_dict["DQCheck"] == "Tier 3"):
                  value  = col_dict["Default"]
                  Flag = "Tier 3; "
                else:
                  Flag = "Tier 2; "

        if (type(value) == str):
          if (col_dict["DQCheck"] != "No"):
              if(col_dict["DataType"] == "int"):
                  err_text = int_check(value,key)
                  if (err_text != "VALID"):
                      err_message.append(err_text)
                      err_col.append(key)
                      Flag = Flag + col_dict["DQCheck"] +"; "
                      if (col_dict["DQCheck"] == "Tier 3"):
                        row_dict[key]  = col_dict["Default"]
              elif(col_dict["DataType"] == "double" or col_dict["DataType"] == "float"):
                  err_text = double_check(value,key)
                  if (err_text != "VALID"):
                      err_message.append(err_text)
                      err_col.append(key)
                      Flag = Flag + col_dict["DQCheck"]+"; "
                      if (col_dict["DQCheck"] == "Tier 3"):
                        row_dict[key]   = col_dict["Default"]
              elif(col_dict["DataType"] == "datetime"):
                  err_text = date_check(value,col_dict["Format"],key)
                  if (err_text != "VALID"):
                      err_message.append(err_text)
                      err_col.append(key)
                      Flag = Flag + col_dict["DQCheck"] +"; "
                      if (col_dict["DQCheck"] == "Tier 3"):
                        row_dict[key]   = col_dict["Default"]
              elif(col_dict["DataType"] == "timestamp"):
                  err_text = timestamp_check(value,col_dict["Format"],key)
                  if (err_text != "VALID"):
                      err_message.append(err_text)
                      err_col.append(key)
                      Flag = Flag + col_dict["DQCheck"]+"; "
                      if (col_dict["DQCheck"] == "Tier 3"):
                        row_dict[key]   = col_dict["Default"]
  
    if(len(err_message)==0):
      err_message.append("VALID")
    err_col = '|'.join(err_col)
    err_message = '|'.join(err_message)
    return err_col,err_message,Flag
  
def create_metadata_dict(df):
  metadata_dict = {}
  for x in list(df.select("ColumnName").collect()):
    metadata_dict[x.ColumnName] = (df.filter(df["ColumnName"] == x.ColumnName).select(
      "DataType","Nullable","DQCheck","Default","Format").first().asDict())
  return metadata_dict

def rowwise_function(row,metadata_dict):
    import pyspark.sql as r
    row_dict = row.asDict()
    err_col,err_message,flag =check_function(row_dict,metadata_dict)
    row_dict.update({'Error_Column':err_col})
    row_dict.update({'Error_Message':err_message})
    row_dict.update({'Flag':flag})
    newrow = r.Row(**row_dict)
    return newrow
def DataqualityCheck(spark,df,df_metadata):
    import pyspark.sql.functions as f
    from pyspark.sql.types import StructType, StringType, IntegerType, StructField
    to_prepend = [StructField("Error_Column", StringType(), True),StructField("Error_Message", StringType(), True),StructField("Flag", StringType(), True)] 
    updated_schema = StructType( to_prepend +df.schema.fields )
    metadata_dict = create_metadata_dict(df_metadata)
    newrdd = df.rdd.map(lambda row: rowwise_function(row,metadata_dict))
    df_checked =spark.createDataFrame(newrdd,updated_schema)
    df_checked = df_checked.select(df.withColumn(
        "Error_Column",f.lit("")).withColumn("Error_Message",f.lit("")).withColumn("Flag",f.lit("")).columns)
    return df_checked,metadata_dict

# COMMAND ----------

def ReadMysql(spark,jdbcDict,PushdownQuery):
  connectionUrl = "jdbc:mysql://{0}/{1}?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC" \
                    .format(jdbcDict['jdbcHostname'], jdbcDict['jdbcDatabasename'])
  connectionProp = {
    "user" : jdbcDict['jdbcUserName'],
    "password" : jdbcDict['jdbcPassword'],
    "driver" : "com.mysql.jdbc.Driver"
   }
  pdq = "({0}) pdq".format(PushdownQuery)
  BronzeDf = spark.read.jdbc(url=connectionUrl, table = pdq , properties = connectionProp)
  return (BronzeDf)


def ReadGoogleDrive(spark,dbutils,ConnectionDict):
  file_id = ConnectionDict["FileId"]
  destination = '/tmp/GoogleDriveFile'
  download_file_from_google_drive(file_id, destination)
  dbutils.fs.mkdirs("dbfs:/test/")
  dbutils.fs.mv('file:/tmp/GoogleDriveFile', "dbfs:/test/")
  BronzeDf = spark.read.format("csv").option("header","true").option("delimiter",ConnectionDict["Delimiter"]).option(
    "inferSchema", "false").load("dbfs:/test/GoogleDriveFile")
  #dbutils.fs.rm("dbfs:/test/GoogleDriveFile",True)
  dbutils.fs.rm("file:/tmp/GoogleDriveFile",True)
  return BronzeDf

def download_file_from_google_drive(id, destination):
    import requests
    URL = "https://docs.google.com/uc?export=download"
    session = requests.Session()
    response = session.get(URL, params = { 'id' : id }, stream = True)
    token = None
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            token = value
    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)
    CHUNK_SIZE = 32768
    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

def ReadOneDrive(spark,dbutils,ConnectionDict):
  URl= ConnectionDict["Link"]
  destination = '/tmp/OneDriveFile'
  download_file_from_one_drive(URl, destination)
  dbutils.fs.mkdirs("dbfs:/test/")
  dbutils.fs.mv('file:/tmp/OneDriveFile', "dbfs:/test/")
  BronzeDf = spark.read.format("csv").option("header","true").option("delimiter",ConnectionDict["Delimiter"]).option(
    "inferSchema", "false").load("dbfs:/test/OneDriveFile")
  #dbutils.fs.rm("dbfs:/test/OneDriveFile",True)
  try:
    dbutils.fs.rm("file:/tmp/OneDriveFile",True)
  except:
    message  = False
  return BronzeDf

def download_file_from_one_drive(URL, destination):
    import requests
    URL = URL
    session = requests.Session()
    response = session.get(URL, stream = True)
    token = None
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            token = value
    if token:
        params = {  'confirm' : token }
        response = session.get(URL, params = params, stream = True)
    CHUNK_SIZE = 32768
    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-
                f.write(chunk)
                
                
def ReadHive(spark,odbcDict,PushdownQuery):
  import pyodbc
  import time as time
  import json
  import os
  import urllib
  import warnings
  import re
  import pandas as pd
 
  #Create the connection to Hive using ODBC
  SERVER_NAME = odbcDict['odbcHostname']
  DATABASE_NAME = odbcDict['odbcDatabasename']
  USERID = odbcDict['odbcUserName']
  PASSWORD = odbcDict['odbcPassword']
  DB_DRIVER="//usr/lib/hive/lib/native/Linux-amd64-64/libhortonworkshiveodbc64.so"  
  driver = 'DRIVER={' + DB_DRIVER + '}'
  server = 'Host=' + SERVER_NAME + ';Port=443'
  database = 'Schema=' + DATABASE_NAME
  hiveserv = 'HiveServerType=2'
  auth = 'AuthMech=6'
  uid = 'UID=' + USERID
  pwd = 'PWD=' + PASSWORD
  CONNECTION_STRING = ';'.join([driver,server,database,hiveserv,auth,uid,pwd])
  connection = pyodbc.connect(CONNECTION_STRING, autocommit=True)
  queryString = """
      {0} ;
  """.format(PushdownQuery)

  pandas_df = pd.read_sql(queryString,connection)
  BronzeDf = sqlContext.createDataFrame(pandas_df)              
  return BronzeDf              
                
    
def ReadAdlsG1(spark,ConnectionDict):
  spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
  spark.conf.set("dfs.adls.oauth2.client.id", "{0}".format(ConnectionDict["ApplicationId"]))
  spark.conf.set("dfs.adls.oauth2.credential","{0}".format(ConnectionDict[""]))
  spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/{0}/oauth2/token".format(ConnectionDict[""]))
  if(ConnectionDict["Format"] == "Parquet"):
    BronzeDf = spark.read.format("parquet").option("header", "true").load(ConnectionDict["Path"])
  elif(ConnectionDict["Format"] == "csv"):
    BronzeDf = spark.read.format("csv").option("header", "true").option("delimiter",ConnectionDict["Delimiter"]).load(ConnectionDict["Path"])
  elif(ConnectionDict["Format"] == "ORC"):
    BronzeDf = spark.read.format("orc").option("header", "true").load(ConnectionDict["Path"])
  else:
    BronzeDf = None
  return BronzeDf    


def ReadBlob(spark,ConnectionDict):
  spark.conf.set(
    "fs.azure.account.key.{0}.blob.core.windows.net".format(ConnectionDict["StorageAccountName"]),ConnectionDict["StorageAccountAccessKey"])
  path = "wasb://{0}@{1}.blob.core.windows.net/".format(ConnectionDict["ContainerName"],ConnectionDict["StorageAccountName"])
  if(ConnectionDict["Format"] == "Parquet"):
    BronzeDf = spark.read.format("parquet").option("header", "true").load(path + ConnectionDict["Path"])
  elif(ConnectionDict["Format"] == "csv"):
    BronzeDf = spark.read.format("csv").option("header", "true").option(
      "delimiter",ConnectionDict["Delimiter"]).load(path + ConnectionDict["Path"])
  elif(ConnectionDict["Format"] == "ORC"):
    BronzeDf = spark.read.format("orc").option("header", "true").load(path + ConnectionDict["Path"])
  else:
    BronzeDf = None
  return BronzeDf
    
    
def ReadFileFromSource(spark,LOGGER,dbutils,cnx,entryid,jdbcUrl,connectionProperties):
  LOGGER.info("================================Data_Ingestion_Intitiated================================")
  pdq_1 = "(select * from `deaccelator`.`datacatlogentry` where `EntryID` = {0} ) pdq_1".format(entryid)
  pdq_2= "(select * from `deaccelator`.`parameter` where `EntryID` = {0} ) pdq_2".format(entryid)
  pdq_3= "(select * from `deaccelator`.`metadata` where `EntryID` = {0} ) pdq_2".format(entryid) 
  datacatlogentry = spark.read.jdbc(url=jdbcUrl, table = pdq_1, properties = connectionProperties)
  parameter = spark.read.jdbc(url=jdbcUrl, table  = pdq_2, properties = connectionProperties)
  MetadataDf = spark.read.jdbc(url  =jdbcUrl, table  = pdq_3, properties = connectionProperties)
  from pyspark.sql.functions import regexp_replace
  MetadataDf = MetadataDf.withColumn("ColumnName",regexp_replace(MetadataDf["ColumnName"]," ",""))
  CatlogDict = datacatlogentry.first().asDict()
  ParameterDict = parameter.first().asDict()
  columns = MetadataDf.select(MetadataDf['ColumnName']).collect()
  column_list = []
  for x in columns:
    column_list.append((x.ColumnName).replace(" ",""))
  from datetime import datetime
  dateTimeObj = (datetime.now())
  start_time = dateTimeObj.strftime("%Y-%m-%d %H-%M-%S.%f")
  
  cur = cnx.cursor()
  querystring ="REPLACE INTO `deaccelator`.`audittable` (`EntryID`,`JobName`,`ProjectName`,`StartTime`,`UserName`,`Status`)VALUES ({0},'{1}','{2}','{3}','{4}','Running')".format(
    CatlogDict["EntryID"],CatlogDict["JobName"],CatlogDict["ProjectName"],start_time,CatlogDict["UserName"])
  cur.execute(querystring)
  cnx.commit()
  cur.close()
  LOGGER.info("Audit Table is updated")
  BronzeDf = ReadFile(spark,LOGGER,dbutils,CatlogDict,ParameterDict,column_list)
  return BronzeDf,MetadataDf
  
  
def ReadFile(spark,LOGGER,dbutils,CatlogDict,ParameterDict,ColumnList):
  ConnectionDict =  eval(ParameterDict["SourceParameter"])
  if(CatlogDict["SourceType"]== "MySql" ):
    LOGGER.info("Source is identified as : MySQL")
    PushdownQuery  = ParameterDict["SourceQuery"]
    BronzeDf = ReadMysql(spark,ConnectionDict,PushdownQuery)
    LOGGER.info("Read Complete")
  elif (CatlogDict["SourceType"]== "Google Drive" ):
    LOGGER.info("Source is identified as : Google Drive")
    BronzeDf = ReadGoogleDrive(spark,dbutils,ConnectionDict)
    LOGGER.info("Read Complete")
  elif (CatlogDict["SourceType"]== "One Drive" ):
    LOGGER.info("Source is identified as : One Drive")
    BronzeDf = ReadOneDrive(spark,dbutils,ConnectionDict)
    LOGGER.info("Read Complete")
  elif (CatlogDict["SourceType"]== "Hive" ):
    LOGGER.info("Source is identified as : Hive")
    PushdownQuery  = ParameterDict["SourceQuery"]
    BronzeDf = ReadHive(spark,ConnectionDict,PushdownQuery)
    LOGGER.info("Read Complete")
  elif (CatlogDict["SourceType"]== "ADLS Gen 1" ):
    LOGGER.info("Source is identified as : ADLS Gen 1")
    BronzeDf = ReadAdlsG1(spark,ConnectionDict)
    LOGGER.info("Read Complete")
  elif (CatlogDict["SourceType"]== "AzureBlob" ):
    LOGGER.info("Source is identified as : AzureBlob")
    BronzeDf = ReadBlob(spark,ConnectionDict)
    LOGGER.info("Read Complete")
  else:
    LOGGER.warn("Unable to identify Source")
    BronzeDf = None
  if(BronzeDf != None):
    BronzeDf = BronzeDf.toDF(*ColumnList)
  return BronzeDf
  
  
def typecast(spark,BronzeDf,metadata_dict):
    import pyspark.sql.functions as f
    if(BronzeDf.filter(BronzeDf["Flag"].contains("Tier 1")).count() == 0):
        SilverDf = BronzeDf.filter(~(BronzeDf["Flag"].contains("Tier 2")))
        RejectedDf = BronzeDf.filter(BronzeDf["Flag"].contains("Tier 2")).drop("Flag")
        for key,value in metadata_dict.items():
          col_dict = metadata_dict[key]
          if (col_dict["DQCheck"] == "Tier 2"):
              if(col_dict["DataType"] == "int"):
                  SilverDf  = SilverDf.withColumn(key,SilverDf[key].cast("int"))
              elif(col_dict["DataType"] == "double" or col_dict["DataType"] == "float") :
                  SilverDf  = SilverDf.withColumn(key,SilverDf[key].cast("double"))
              elif(col_dict["DataType"] == "datetime" ):
                  format_date = formatconversion(col_dict["Format"])
                  SilverDf  = SilverDf.withColumn(key,f.to_date(SilverDf[key],format_date))
              elif(col_dict["DataType"] == "timestamp" ):
                  format_date = formatconversion(col_dict["Format"])
                  SilverDf = SilverDf.withColumn(key,f.to_timestamp(SilverDf[key],format_date))		
                
        if(SilverDf.filter(SilverDf["Flag"].contains("Tier 3")).count() == 0):
            SilverDf = SilverDf.drop("Error_Column","Error_Message","Flag")
        else:
            SilverDf = SilverDf.drop("Flag")
    else:
        RejectedDf = BronzeDf.drop("Error_Column","Error_Message","Flag")
        SilverDf = spark.createDataFrame([],RejectedDf.schema)
    return SilverDf,RejectedDf
	 
    
    
  
def CheckDuplicates(spark,entryid,cnx,BronzeDf,MetadataDf):
  primary_key = MetadataDf.select(MetadataDf['ColumnName']).filter(MetadataDf['Primarykey'] == 'Yes')
  TotalRows = BronzeDf.count()
  BronzeDf = BronzeDf.dropDuplicates()
  DuplicateRecords = TotalRows -  BronzeDf.count()
  if(primary_key.count() !=0):
    primary_key = (primary_key.first()).asDict()
    PrimaryKeyColumn=primary_key["ColumnName"]
    BronzeDf = BronzeDf.dropDuplicates([PrimaryKeyColumn])
    DuplicateRecords = TotalRows -  BronzeDf.count()
  DQCheckFailed = BronzeDf.filter(~(BronzeDf["Flag"] == '')).count()
  import mysql.connector
  cnx = mysql.connector.connect(
    user="DEadmin@demetadata", password="Tredence@123", host="demetadata.mysql.database.azure.com",database='deaccelator')
  cur = cnx.cursor()
  querystring ="update `deaccelator`.`audittable` set TotalRows={0},DuplicateRecords={1},DQCheckFailed={2} where EntryID = {3}".format(TotalRows,DuplicateRecords,DQCheckFailed,entryid)
  cur.execute(querystring)
  cnx.commit()
  cur.close()
  return BronzeDf

    
def formatconversion(format_date):
    format_for_date = format_date.replace("%a","E").replace("%A","EEEE").replace("%d","dd").replace(
        "%-d","d").replace("%B","MMMM").replace("%b","MMM").replace("%m","MM").replace("%-m","M").replace(
        "%y","yy").replace("%Y","yyyy").replace("%H","kk").replace("%-H","k").replace("%I","KK").replace("%-I","K").replace(
        "%p","aa").replace("%M","mm").replace("%-M","m").replace("%S","ss").replace("%-S","s").replace("%Z","zz").replace(
        "%W","ww")
    return format_for_date
	

def WriteFileToTarget(spark,cnx,entryid,jdbcUrl,connectionProperties,SilverDf,RejectedDf,Reject2Df,Reject3Df):
  pdq_1 = "(select * from `deaccelator`.`datacatlogentry` where `EntryID` = {0} ) pdq_1".format(entryid)
  pdq_2= "(select * from `deaccelator`.`parameter` where `EntryID` = {0} ) pdq_2".format(entryid)
  datacatlogentry = spark.read.jdbc(url=jdbcUrl, table = pdq_1, properties = connectionProperties)
  parameter = spark.read.jdbc(url=jdbcUrl, table  = pdq_2, properties = connectionProperties)
  CatlogDict = datacatlogentry.first().asDict()
  ParameterDict = parameter.first().asDict()
  if(CatlogDict["TargetType"]== "ADLS Gen 1" ):
    path = WriteAzure(spark,SilverDf,RejectedDf,Reject2Df,Reject3Df,ParameterDict,CatlogDict)
  else:
    IngestionMessage =  "Unsupported Target"
    path = ''
  IngestedRows = SilverDf.count()
  RejectedRows = RejectedDf.count()
  brfailed  = Reject2Df.count()
  crulefailed = Reject3Df.count()
  cnx = mysql.connector.connect(
    user="DEadmin@demetadata", password="Tredence@123", host="demetadata.mysql.database.azure.com",database='deaccelator')
  cur = cnx.cursor()
  querystring ="update `deaccelator`.`audittable` set IngestedRows = '{0}',RelativeFilePath = '{1}',RejectedRows = '{2}',BRCheckFailed = '{3}' ,CRCheckFailed =  {5} where EntryId = {4}".format(
    IngestedRows,path,RejectedRows,brfailed,entryid,crulefailed)
  cur.execute(querystring)
  cnx.commit()
  cur.close()
  
    
def WriteAzure(spark,SilverDf,RejectedDf,Reject2Df,Reject3Df,ParameterDict,CatlogDict):
  import pyspark.sql
  from datetime import datetime
  ConnectionDict =  eval(ParameterDict["TargetParameter"])
  spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
  spark.conf.set("dfs.adls.oauth2.client.id", "{0}".format(ConnectionDict["ApplicationID"]))
  spark.conf.set("dfs.adls.oauth2.credential","{0}".format(ConnectionDict["ApplicationCredential"]))
  spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/{0}/oauth2/token".format(ConnectionDict["DirectoryID"]))
  dateTimeObj = (datetime.now())
  current_time = dateTimeObj.strftime("%H-%M-%S.%f")
  date_path ="YYYY=" +dateTimeObj.strftime("%Y") + ("/MM=") + dateTimeObj.strftime("%m") + ("/DD=") + dateTimeObj.strftime("%d") 
  output_path = "adl://{0}.azuredatalakestore.net/{1}/{2}/{3}/OUTPUT/".format(
    ConnectionDict["adlAccountName"],CatlogDict["ProjectCategory"], CatlogDict["ProjectName"],CatlogDict["JobName"]) + date_path 
  reject_path = "adl://{0}.azuredatalakestore.net/{1}/{2}/{3}/REJECT/".format(
    ConnectionDict["adlAccountName"],CatlogDict["ProjectCategory"], CatlogDict["ProjectName"],CatlogDict["JobName"])+ date_path  + "/Rejection_" + current_time
  reject_path2 = "adl://{0}.azuredatalakestore.net/{1}/{2}/{3}/BUSINESS_REJECT/".format(
    ConnectionDict["adlAccountName"],CatlogDict["ProjectCategory"], CatlogDict["ProjectName"],CatlogDict["JobName"])+ date_path  + "/BusinessRejection_" + current_time
  reject_path3 = "adl://{0}.azuredatalakestore.net/{1}/{2}/{3}/CUSTOM_REJECT/".format(
    ConnectionDict["adlAccountName"],CatlogDict["ProjectCategory"], CatlogDict["ProjectName"],CatlogDict["JobName"])+ date_path  + "/CustomRejection_" + current_time
  IngestionMessage = "Succesful"
  if(ParameterDict["TargetFileType"] == "Parquet"):
    SilverDf.write.format("parquet").mode(CatlogDict["Operation"]).option("header", "true").save(output_path)
  elif(ParameterDict["TargetFileType"] == "Flatfiles"):
    SilverDf.write.format("csv").mode(CatlogDict["Operation"]).option(
      "header", "true").option("delimiter",ParameterDict["TargetFileDelimiter"]).save(output_path)
  elif(ParameterDict["TargetFileType"] == "ORC"):
    SilverDf.write.format("orc").mode(CatlogDict["Operation"]).option("header", "true").save(output_path)
  else:
    IngestionMessage = "Unsuccesful"
  out_path = "/{0}/{1}/{2}/OUTPUT/".format(
    CatlogDict["ProjectCategory"], CatlogDict["ProjectName"],CatlogDict["JobName"]) + date_path
  rej_path = "/{0}/{1}/{2}/REJECT/".format(
    CatlogDict["ProjectCategory"], CatlogDict["ProjectName"],CatlogDict["JobName"]) + date_path  +"/Rejection_" + current_time  
  rej_path2 = "/{0}/{1}/{2}/BUSINESS_REJECT/".format(
    CatlogDict["ProjectCategory"], CatlogDict["ProjectName"],CatlogDict["JobName"]) + date_path  +"/BusinessRejection_" + current_time  
  rej_path3 = "/{0}/{1}/{2}/CUSTOM_REJECT/".format(
    CatlogDict["ProjectCategory"], CatlogDict["ProjectName"],CatlogDict["JobName"]) + date_path  +"/CustomRejection_" + current_time  

  if(RejectedDf.count()!= 0):
    RejectedDf.coalesce(1).write.format("csv").mode(CatlogDict["Operation"]).option("header", "true").save(reject_path)
  if(Reject2Df.count()!= 0):
    Reject2Df.coalesce(1).write.format("csv").mode(CatlogDict["Operation"]).option("header", "true").save(reject_path2)
  if(Reject3Df.count()!= 0):
    Reject3Df.coalesce(1).write.format("csv").mode(CatlogDict["Operation"]).option("header", "true").save(reject_path3)
  op = "None"
  rp = "None"
  brp = "None"
  crp = "None"
  if(SilverDf.count() != 0):
    op = out_path
  if(RejectedDf.count()!= 0):
    rp = rej_path
  if(Reject2Df.count()!= 0):
    brp = rej_path2
  if(Reject3Df.count()!= 0):
    crp = rej_path3
  path = "OUTPUT PATH : {0} | REJECT OUTPUT PATH : {1} | BUSINESS REJECT OUTPUT PATH : {2} | CUSTOM REJECT OUTPUT PATH : {3}".format(op,rp,brp,crp)
  return path

# COMMAND ----------

def create_list_from_string(string):
  list1=string.split(',')
  return list1
def IsEmail(email,rand_string):
  #rand_string is not useful
  regex = '^[A-Za-z0-9!-|]+[\._]?[A-Za-z0-9]+[@]\w+[.]\w{2,3}$'
  regex2 = '^[A-Za-z0-9!-|]+[\._]?[A-Za-z0-9]+[@]\w+[.]\w+[.]\w{2,3}$'
  if(re.search(regex,email)):
    return True
  elif(re.search(regex2,email)):
    return True      
  else:
    return False

def IsMobileNumber(elem,rand_string):
  #,rand_string is not useful
  return bool(re.match(r'^(?:\d{10}|\d{3}-\d{3}-\d{4})$',str(elem)))

def Range(num,list2):
  list1=create_list_from_string(list2)
  try:
    if num in range(int(list1[0]),int(list1[1])):
      return True
    else:
      return False
  except:
    return False


def IsCreditCardNumber(num,rand_string):
  #,rand_string is not useful
  return bool(re.match(r'^([3456][0-9]{3})-?([0-9]{4})-?([0-9]{4})-?([0-9]{4})$',str(num)))

def IsSSN(ssn,rand_string):
  #,rand_string is not useful
  ssn = ssn.split("-")
  if map(len, ssn) != [3,2,4]:
    return False
  elif any(not x.isdigit() for x in ssn):
    return False
  return True

def IsURL(elem,rand_string):
  #,rand_string is not usefu
  regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
  return bool(re.match(regex,str(elem)))

def IsPinCode(pinCode,rand_string):
  #,rand_string is not useful
  regex = "^[1-9]{1}[0-9]{2}\\s{0,1}[0-9]{3}$";
  p = re.compile(regex); 
  if (pinCode == ''):
    return False;
  m = re.match(p, pinCode);
  if m is None:
    return False
  else:
    return True

def GreaterThan(elem,list1):
  try:
    if elem>int(list1):
      return True
    else:
      return False
  except:
    return False
      
def LesserThan(elem,list1):
  try:
    if elem < int(list1):
      return True
    else:
      return False
  except:
    return False
  

def listofvalues(elem,list1):
    if elem in list1:
        return True
    else:
        return False

# def lookup(a,b,c,d):
# #   global lookup_df
#   try:
#     e=lookup_df_main[lookup_df_main[c] == str(a)][d].iloc[0]
#     if e==b:
#       return True
#     else:
#       return False
#   except:
#     return False
def lookup(a,b,c,d):
  try:
    for x in lookuprdd.value:
      if(x[c]==str(a)):
        ele = x[d]
    #ls = xc.select(xc[d]).where(xc[c]==str(a)).first()
    if ele==b:
      return True
    else:
      return False
  except:
    return False


      
      
def encrypt(message,key = "9D7871849D78719A9D78719F"):
    message = str(message)
    from Crypto.Cipher import DES3
    import base64
    iv = b'\xe5\x1f~\xf0}\x8e\xedG'
    cipher_encrypt = DES3.new(key, DES3.MODE_OFB, iv)
    if (len(message) % 8 ==0):
        encrypted_data = cipher_encrypt.encrypt(message.encode())
        return base64.b64encode(bytes(encrypted_data)).decode()
    else:
        for i in range(len(message) % 8 ,8):
            message = message + ' '
        encrypted_data = cipher_encrypt.encrypt(message.encode())
        return base64.b64encode(bytes(encrypted_data)).decode()

                             
       
                                 
# a dictionary and use it accordingly

def call_func(x, y, func):
  dispatcher = { 'GreaterThan' : GreaterThan, 'LesserThan' : LesserThan,
             'listofvalues':listofvalues,'IsPinCode':IsPinCode,'IsURL':IsURL,
             'IsSSN':IsSSN,'IsCreditCardNumber':IsCreditCardNumber,'Range':Range,
             'IsMobileNumber':IsMobileNumber,'IsEmail':IsEmail,'Encrypt':encrypt,'Lookup':lookup}# depending on number of input parameters for afunctiocreate 
  try:
    return dispatcher[func](x, y)
  except:
    return False


      
  
def row_function(row,parameter):
  import pyspark.sql
  rowdict = row.asDict()
  flag = []
  for x in parameter:
    if(x['RuleName'] == 'Encrypt'):
      value = encrypt(rowdict[x["ColumnName"]],x["RuleParameters"])
      rowdict[x["ColumnName"]]=value
    elif(x['RuleName'] == 'Lookup'):
      lookup_list=x['RuleParameters'].split(',',3)
      a = rowdict[lookup_list[0]]
      b = rowdict[x["ColumnName"]]
      c = lookup_list[1]
      d = lookup_list[2]
      if(lookup(a,b,c,d)):
        flag = flag
      else:
        flag.append(x['RuleName']+"-"+': Failed')   # x[ruleparaameter ] not necessary here 
    elif(call_func(rowdict[x["ColumnName"]],x["RuleParameters"], x['RuleName'])):
      flag = flag
    else:
      flag.append(x['RuleName']+"-"+x['RuleParameters'] +': Failed')
      
    
  if(len(flag)==0):
    flag.append("VALID")
  flag = '|'.join(flag)
  rowdict.update({'Flag':flag})
  newrow = pyspark.sql.Row(**rowdict)
  return newrow

def Business_Rule_Check(spark,jdbcUrl,connectionProperties,df,entryid):
  pdq_1 = "(select * from `deaccelator`.`business_rule_metadata` where EntryID = {0}  ) pdq_1".format(entryid)
  df_rule = spark.read.jdbc(url=jdbcUrl, table = pdq_1, properties = connectionProperties)
  from pyspark.sql.functions import regexp_replace
  df_rule = df_rule.withColumn("ColumnName",regexp_replace(df_rule["ColumnName"]," ",""))
  lookup_df = spark.createDataFrame([],df_rule.schema)
  parameter = []
  rddobject = df_rule.rdd
  for x in rddobject.collect():
    adict = x.asDict()
    parameter.append(adict)
    
    for x in parameter:
      if (x['RuleName']=='Lookup'):
        lookuplist=x['RuleParameters'].split(',',3)
        connection_dictionary=eval(lookuplist[3])
        lookup_df=ReadBlob(spark,connection_dictionary)
      else:
        lookup_df = spark.createDataFrame([],df_rule.schema)
  global lookuprdd
  lookuprdd = sc.broadcast(lookup_df.collect())
  import pyspark.sql.functions as f
  from pyspark.sql.types import StructType, StringType, IntegerType, StructField
  newrdd = df.rdd.map(lambda row: row_function(row,parameter))
  to_prepend = [StructField("Flag", StringType(), True)] 
  updated_schema = StructType( to_prepend +df.schema.fields )
  schema_sorted = StructType()
  structfield_list_sorted = sorted(updated_schema, key=lambda x: x.name)
  for item in structfield_list_sorted:
    schema_sorted.add(item)
  newdf = spark.createDataFrame(newrdd,schema_sorted)
  silver_df=newdf.filter(newdf['Flag']=='VALID')
  silver_df=silver_df.drop('Flag')
  silver_df = silver_df.select(list(Metadata_Dict.keys()))
  reject_df=newdf.filter(newdf['Flag']!='VALID')
  return silver_df,reject_df


# COMMAND ----------

def row_function_1(row,list1):
  import pyspark.sql
  rowdict = row.asDict()
  flag = []
  for x in list1:
    if rowdict[x]==0:
      flag.append(x+': Failed')
    else:
      flag = flag
  if(len(flag)==0):
    flag.append("VALID")
  flag = '|'.join(flag)
  rowdict.update({'Flag':flag})
  newrow = pyspark.sql.Row(**rowdict)
  return newrow

def custom_rule_check(spark,jdbcUrl,connectionProperties,FinalDf,entryid):
  import pyspark.sql.functions as f
  from pyspark.sql.types import StructType, StringType, IntegerType, StructField
  import mysql.connector
  pdq_1 = "(select Custom_rule from `deaccelator`.`custom_rule_metadata` where `EntryID` = {0} ) as pdq".format(entryid)
  df_custom= spark.read.jdbc(url=jdbcUrl, table = pdq_1, properties = connectionProperties)
  try:
    customdict = df_custom.first().asDict()
    customlist=customdict["Custom_rule"].split(',')
    for item in customlist:
      pdq_3 = "(select Definition from `deaccelator`.`centralrulerepo` where `Name` = '{0}' )".format(item)
      cnx = mysql.connector.connect(user="DEadmin@demetadata", password="Tredence@123", host="demetadata.mysql.database.azure.com",database='deaccelator')
      cur = cnx.cursor()
      cur.execute(pdq_3 )
      rule_definition=(cur.fetchone())[0]
      FinalDf=FinalDf.select(f.col("*"),f.expr(rule_definition).alias(item))
    newrdd1= FinalDf.rdd.map(lambda row: row_function_1(row,customlist))
    to_prepend = [StructField("Flag", StringType(), True)] 
    updated_schema = StructType( to_prepend +FinalDf.schema.fields )
    schema_sorted = StructType()
    structfield_list_sorted = sorted(updated_schema, key=lambda x: x.name)
    for item in structfield_list_sorted:
      schema_sorted.add(item)
    newdf = spark.createDataFrame(newrdd1,schema_sorted)
  except:
    newdf=FinalDf.withColumn("Flag",f.lit("VALID"))
  silver_df=newdf.filter(newdf['Flag']=='VALID')
  silver_df=silver_df.drop('Flag')
  silver_df = silver_df.select(list(Metadata_Dict.keys()))
  reject_df=newdf.filter(newdf['Flag']!='VALID')
  return silver_df,reject_df         

# COMMAND ----------

#entryid = dbutils.widgets.get("EntryId")
entryid = 545

# COMMAND ----------

#connection parameters for sql db
jdbcHostname = 'demetadata.mysql.database.azure.com'
jdbcUserName = 'DEadmin@demetadata'
jdbcPassword = 'Tredence@123'
jdbcDatabasename = 'deaccelator'

jdbcUrl = "jdbc:mysql://{0}/{1}?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC".format(jdbcHostname, jdbcDatabasename)

connectionProperties = {
  "user" : jdbcUserName,
  "password" : jdbcPassword ,
  "driver" : "com.mysql.jdbc.Driver"
}
import mysql.connector
cnx = mysql.connector.connect(
  user="DEadmin@demetadata", password="Tredence@123", host="demetadata.mysql.database.azure.com",database='deaccelator')

log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger("___MY_CUSTOM_LOG___")

# COMMAND ----------

try:
  BronzeDf,MetadataDf = ReadFileFromSource(spark,LOGGER,dbutils,cnx,entryid,jdbcUrl,connectionProperties)
  try:
    CheckedDf,Metadata_Dict = DataqualityCheck(spark,BronzeDf,MetadataDf)
    try:
      CleanedDf = CheckDuplicates(spark,entryid,cnx,CheckedDf,MetadataDf)
      try:
        SilverDf,RejectedDf = typecast(spark,CleanedDf,Metadata_Dict)
        try: 
          FinalDf,BusRejDf = Business_Rule_Check(spark,jdbcUrl,connectionProperties,SilverDf,entryid)
          try:
            GoldDF,CustRejDf = custom_rule_check(spark,jdbcUrl,connectionProperties,FinalDf,entryid)
            try:
              WriteFileToTarget(spark,cnx,entryid,jdbcUrl,connectionProperties,GoldDF,RejectedDf,BusRejDf,CustRejDf)
              log_message = "Ingestion Successful"
            except:
              log_message = "Unable to Ingest Data"
          except:
            log_message = "Custom Rule Check Failed"
        except:
          log_message = "Business Rule Check Failed"
      except:
        log_message = "Data Type Not Supported"
    except:
      log_message = "Duplicate Check Failed"
  except:
    log_message = "DQ Check Failed"
except:
  log_message = "Unable to Read the File"

# COMMAND ----------

log_message

# COMMAND ----------

from datetime import datetime
dateTimeObj = (datetime.now())
end_time = dateTimeObj.strftime("%Y-%m-%d %H-%M-%S.%f")
import mysql.connector
cnx = mysql.connector.connect(
  user="DEadmin@demetadata", password="Tredence@123", host="demetadata.mysql.database.azure.com",database='deaccelator')
cur = cnx.cursor()
querystring ="update `deaccelator`.`audittable` set EndTime = '{0}',Status = '{1}' where EntryID = {2}".format(end_time,log_message,entryid)
cur.execute(querystring)
cnx.commit() 
cur.close()
if (log_message != 'Ingestion Successful'):
  qs = "update `deaccelator`.`audittable` set RelativeFilePath ='OUTPUT PATH : None | REJECT OUTPUT PATH : None | BUSINESS REJECT OUTPUT PATH : None | CUSTOM REJECT OUTPUT PATH : None'  where EntryID = {0}".format(entryid)
  cur = cnx.cursor()
  cur.execute(qs)
  cnx.commit() 
  cur.close()

# COMMAND ----------

