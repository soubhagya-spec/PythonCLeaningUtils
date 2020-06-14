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
        if(in_str.strip().lower() == "null" or in_str.strip().lower() == "nan" or in_str.strip().lower() == "null" or len(in_str.strip())==0 ):
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
def DataqualityCheck(df,df_metadata):
    import pyspark.sql.functions as f
    from pyspark.sql.types import StructType, StringType, IntegerType, StructField
    to_prepend = [StructField("Error_Column", StringType(), True),StructField("Error_Message", StringType(), True),StructField("Flag", StringType(), True)] 
    updated_schema = StructType( to_prepend +df.schema.fields )
    metadata_dict = create_metadata_dict(df_metadata)
    newrdd = df.rdd.map(lambda row: rowwise_function(row,metadata_dict))
    df_checked =spark.createDataFrame(newrdd,updated_schema)
    df_checked = df_checked.select(BronzeDf.withColumn(
        "Error_Column",f.lit("")).withColumn("Error_Message",f.lit("")).withColumn("Flag",f.lit("")).columns)
    return df_checked,metadata_dict