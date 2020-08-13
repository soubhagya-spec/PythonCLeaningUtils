# Databricks notebook source
#dbutils.widgets.remove("entryid")

# COMMAND ----------

entryid=dbutils.widgets.get("entryid")

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

def Range(num,list1):
  list2=create_list_from_string(list1)
  if num in range(int(list2[0]),int(list2[1])):
    return True
  else:
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
    if elem > int(list1):
        return True
    else:
        return False
      
def LesserThan(elem,list1):
    if elem < int(list1):
        return True
    else:
        return False
def listofvalues(elem,list1):
    if elem in list1:
        return True
    else:
        return False

# COMMAND ----------

dispatcher = { 'GreaterThan' : GreaterThan, 'LesserThan' : LesserThan,
             'listofvalues':listofvalues,'IsPinCode':IsPinCode,'IsURL':IsURL,
             'IsSSN':IsSSN,'IsCreditCardNumber':IsCreditCardNumber,'Range':Range,
             'IsMobileNumber':IsMobileNumber,'IsEmail':IsEmail}  # depending on number of input parameters for afunction create 
                                                                        # a dictionary and use it accordingly

def call_func(x, y, func):
    try:
        return dispatcher[func](x, y)
    except:
        return "Invalid function"



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

# COMMAND ----------

pdq_1 = "(select * from `deaccelator`.`business_rule_metadata` where EntryID = {0}  ) pdq_1".format(entryid)
df_rule = spark.read.jdbc(url=jdbcUrl, table = pdq_1, properties = connectionProperties)

# COMMAND ----------

pdq_1 = "(select * from `testdb`.`actor`) pdq_1"
df = spark.read.jdbc(url=jdbcUrl, table = pdq_1, properties = connectionProperties)

# COMMAND ----------

df_rule.show()

# COMMAND ----------

parameter = []
rddobject = df_rule.rdd
for x in rddobject.collect():
  adict = x.asDict()
  parameter.append(adict)

# COMMAND ----------

df.show(5)

# COMMAND ----------

def GreaterThan(elem,list1):
    if elem > int(list1):
        return True
    else:
        return False

# COMMAND ----------

def rowwise_function(row,parameter):
  import pyspark.sql
  rowdict = row.asDict()
  flag = []
  for x in parameter:
    #flag.append(x["RuleName"]+':'+str(call_func(rowdict[x["ColumnName"]],x["RuleParameters"], x['RuleName'])))
    if(eval(x['RuleName']+'({0},{1})'.format(rowdict[x['ColumnName']],x['RuleParameters']))):
      flag=flag
    else:
      flag.append(x['RuleName']+"-"+x['RuleParameters'] +': Failed')
    #flag.append(x['RuleName']+':'+str(eval(x['RuleName']+'({0},{1})'.format(rowdict[x['ColumnName']],x['RuleParameters']))))
  if(len(flag)==0):
    flag.append("VALID")
  flag = '|'.join(flag)
  rowdict["Flag"]= flag
  newrow = pyspark.sql.Row(**rowdict)
  return newrow

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
newrdd = df.rdd.map(lambda row: rowwise_function(row,parameter))
to_prepend = [StructField("Flag", StringType(), True)] 
updated_schema = StructType( to_prepend +df.schema.fields )
newdf = spark.createDataFrame(newrdd,updated_schema)

# COMMAND ----------

display(newdf)

# COMMAND ----------

newrdd2=[]
for row in df.rdd.collect():
  newrdd2.append(rowwise_function(row,parameter))

# COMMAND ----------

newdf2 = spark.createDataFrame(newrdd2,updated_schema)

# COMMAND ----------

display(newdf2)

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

def Range(num,list1):
  list2=create_list_from_string(list1)
  if num in range(int(list2[0]),int(list2[1])):
    return True
  else:
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
    if elem > int(list1):
        return True
    else:
        return False
      
def LesserThan(elem,list1):
    if elem < int(list1):
        return True
    else:
        return False
def listofvalues(elem,list1):
    if elem in list1:
        return True
    else:
        return False
      
  
def rowwise_function(row,parameter):
  import pyspark.sql
  rowdict = row.asDict()
  flag = []
  for x in parameter:
    #flag.append(x["RuleName"]+':'+str(call_func(rowdict[x["ColumnName"]],x["RuleParameters"], x['RuleName'])))
    if(eval(x['RuleName']+'({0},{1})'.format(rowdict[x['ColumnName']],x['RuleParameters']))):
      flag=flag
    else:
      flag.append(x['RuleName']+"-"+x['RuleParameters'] +': Failed')
    #flag.append(x['RuleName']+':'+str(eval(x['RuleName']+'({0},{1})'.format(rowdict[x['ColumnName']],x['RuleParameters']))))
  if(len(flag)==0):
    flag.append("VALID")
  flag = '|'.join(flag)
  rowdict["Flag"]= flag
  newrow = pyspark.sql.Row(**rowdict)
  return newrow

def Business_Rule_Ceck(spark,jdbcUrl,connectionProperties,df,entryid):
  pdq_1 = "(select * from `deaccelator`.`business_rule_metadata` where EntryID = {0}  ) pdq_1".format(entryid)
  df_rule = spark.read.jdbc(url=jdbcUrl, table = pdq_1, properties = connectionProperties)
  parameter = []
  rddobject = df_rule.rdd
  for x in rddobject.collect():
    adict = x.asDict()
    parameter.append(adict)
  import pyspark.sql.functions as f
  from pyspark.sql.types import StructType, StringType, IntegerType, StructField
  #newrdd = df.rdd.map(lambda row: rowwise_function(row,parameter))
  to_prepend = [StructField("Flag", StringType(), True)] 
  updated_schema = StructType( to_prepend +df.schema.fields )
  newrdd=[]
  for row in df.rdd.collect():
    newrdd.append(rowwise_function(row,parameter))
  newdf = spark.createDataFrame(newrdd,updated_schema)
  silver_df=newdf.filter(newdf['Flag']=='VALID')
  silver_df=silver_df.drop('Flag')
  reject_df=newdf.filter(newdf['Flag']!='VALID')
  return silver_df,reject_df


# COMMAND ----------

a,b = Business_Rule_Ceck(spark,jdbcUrl,connectionProperties,df,entryid)

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

rule = "case when actor_id > 100 then 1 else 0 end"
rule_name = "CustomRule_"+"New_Column"

# COMMAND ----------

df.select(f.col("*"),f.expr(rule).alias(rule_name)).show()

# COMMAND ----------

df.show()

# COMMAND ----------

spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id","3db3402c-3299-4a05-adee-d0435636b488")
spark.conf.set("dfs.adls.oauth2.credential","S/h-UfisoimzFMYxH_S/rHpY3vXnc756")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/927e65b8-7ad7-48db-a3c6-c42a67c100d6/oauth2/token")
path = "adl://dataingestiondatalake.azuredatalakestore.net/PythonLog/Log"

# COMMAND ----------

dbutils.fs.head("dbfs:/databricks/driver/example.log")

# COMMAND ----------

import logging
logging.basicConfig(filename='example.log', filemode='w', level=logging.DEBUG)
logging.debug('This message should go to the log file')
logging.info('So should this')
logging.warning('And this, too')

# COMMAND ----------

df = spark.read.format("csv").option("delimiter",",").load("dbfs:/cluster-logs/0417-064140-cask871/driver/log4j-active.log")

# COMMAND ----------

df_log = df.filter(df['_c0'].contains('___MY_CUSTOM_LOG___:'))

# COMMAND ----------

display(df_log)

# COMMAND ----------

dflog = df_log.withColumn("_cx",f.concat("_c0",f.lit(" "),"_c1",f.lit(" "),"_c2",f.lit(" "),"_c3",f.lit(" "),"_c4"))

# COMMAND ----------

display(dflog)

# COMMAND ----------

dflog.select("_cx").coalesce(1).write.format("text").mode("append").option("delimiter"," ").option("header", "false").save(path+"/test.txt")

# COMMAND ----------

log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger("___MY_CUSTOM_LOG___")

# COMMAND ----------



# COMMAND ----------

LOGGER.info("Soubhagya_Dash_started_the_logger")
LOGGER.warn("Soubhagya_Dash_warned_the_logger")
LOGGER.error("Soubhagya_Dash_Found_some_error_the_logger")
LOGGER.fatal("Soubhagya_Dash_told_the_logger_its_a_fatal_error")

# COMMAND ----------

# MAGIC %sh cat /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties

# COMMAND ----------

df = spark.read.format("csv").option("delimiter"," ").load("dbfs:/cluster-logs/0417-064140-cask871/driver/log4j-active.log")
df.filter(df['_c3']=='___MY_CUSTOM_LOG___:').show()

# COMMAND ----------

dbutils.notebook.run("Demo", 60, {"EntryID": 545})