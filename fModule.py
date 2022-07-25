import pandas as pd
import numpy as np
import sys
pd.options.display.max_rows = 80
pd.options.display.max_columns = 150
pd.options.display.max_colwidth = 100
pd.options.display.float_format = "{:.1f}".format # Number round to 1 decimal places

import datetime
from datetime import timedelta

Today = datetime.date.today().strftime('%Y%m%d')
date_today = datetime.date.today()
Now_time = datetime.datetime.now()
CurrentMth = (datetime.date.today()).strftime('%Y%m')
edwMth = (datetime.date.today() - timedelta(days=1)).strftime('%Y%m')
edwToday = (datetime.date.today() - timedelta(days=1)).strftime('%Y%m%d')
date_edwToday = datetime.date.today() - timedelta(days=1)
edwTodayAccess = (datetime.date.today() - timedelta(days=1)).strftime('%Y,%m,%d')
# ========================================================================================================================================
def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)
def first_day_of_month(any_day):
    return any_day.replace(day=1)
# ========================================================================================================================================
def str_todate(str_date,date_format):
    
    if date_format != None:
        datetime_date = datetime.datetime.strptime(str_date, date_format).date()
    else:
        datetime_date = datetime.datetime.strptime(str_date, '%Y-%m-%d').date()

    return datetime_date
# ========================================================================================================================================
import teradatasql as tsql
def get_edw_conn_obj():
    return tsql.connect(None, host='10.0.0.0', user='T159', password='ENCRYPTED_PASSWORD(file:D:\PassKey.properties,file:D:\EncPass.properties)')

def Teradata(sql_query):
    conn = get_edw_conn_obj()
    print("EDW query begin: ",str(datetime.datetime.now()))
    
    try:
        df_query = pd.read_sql(sql_query,conn)
        df_query.columns = df_query.columns.str.strip().str.lower().str.replace(' ', '_')
        print(df_query.shape)
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error")   
    finally:
        print("EDW query end: ",str(datetime.datetime.now()))
        conn.close()
    
    return df_query
# ========================================================================================================================================
def Teradata_execute(sql_query):
    conn = get_edw_conn_obj()

    try:
        cuxr = conn.cursor()
        print(cuxr.execute(sql_query))
    except:
        print("EDW Error")    
    finally:
        conn.close()
    
    return obj_result
# ========================================================================================================================================
def get_EDW_data_date():
    return Teradata("select Curr_Date from PRD_DMTP_MSO_VW.dmart_as_of_date where Dmart_Type like 'GENERAL'")['curr_date'][0]
# ========================================================================================================================================
import pyodbc as msaccess

def MS_Access(file,sql):
    cnxn = msaccess.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+file+';')
    df_MS_Query = pd.DataFrame()
    
    try:
        df_MS_Query = pd.read_sql(sql,cnxn)
        df_MS_Query.columns = df_MS_Query.columns.str.strip().str.lower().str.replace(' ', '_')
        print(df_MS_Query.shape)
    except msaccess.DatabaseError as e:
        print("** pyodbc exception occurs ** "+str(e))
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error")
    finally:      
        cnxn.close()
    
    return df_MS_Query
# ==============================================
def Teradata_ODBC(sql):
    cnxn = msaccess.connect('DSN=sun4980')
    df_EDW_Query = pd.DataFrame()
    print("EDW query begin: ",str(datetime.datetime.now()))
    
    try:
        df_EDW_Query = pd.read_sql(sql,cnxn)
        df_EDW_Query.columns = df_EDW_Query.columns.str.strip().str.lower().str.replace(' ', '_')
        print(df_EDW_Query.shape)
    except msaccess.DatabaseError as e:
        print("** pyodbc exception occurs ** "+str(e))
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error")
    finally:
        print("EDW query end: ",str(datetime.datetime.now()))
        cnxn.close()

    return df_EDW_Query
# ========================================================================================================================================
def pandas_dtypes(df_temp,dateCols,int64Cols,intCols,date_format):
    for x in dateCols:
        print(x)
        #df_temp[x].fillna('1900-01-01',inplace=True)
        df_temp[x] = df_temp[x].fillna(np.nan)
        df_temp[x] = pd.to_datetime(df_temp[x],errors='raise',format = date_format)

    for x in int64Cols:
        print(x)
        df_temp[x] = df_temp[x].fillna(-1)
        df_temp[x] = df_temp[x].astype('int64')    
    
    for x in intCols:
        print(x)
        df_temp[x] = df_temp[x].fillna(-1)
        df_temp[x] = df_temp[x].astype(int)   

    print(df_temp.dtypes)
    return
# ========================================================================================================================================
def pandas_str_col(df_temp,arr_strCols):
    for x in arr_strCols:
        print("to_string:",x)
        df_temp[x] = df_temp[x].astype(str)

    return
# ================================================================================================================
def df_print(df_tmp):
    print(df_tmp.shape)
    return
# ========================================================================================================================================
def pandas_date_notnull(df_temp,dateCols,date_format):
    for x in dateCols:
        print("Datetime64 fill 1900-01-01:",x)
        df_temp[x] = df_temp[x].fillna('1900-01-01')
        df_temp[x] = pd.to_datetime(df_temp[x],format = date_format)
        
    return
# ========================================================================================================================================
def pandas_int_fillzero(df_temp,intCols,int_type):
    for x in intCols:
        print("INT fill 0:",x)
        df_temp[x] = df_temp[x].fillna(0)
        df_temp[x] = df_temp[x].astype(int_type)
        
    return
# ========================================================================================================================================
def pandas_cat_dtypes(df_temp,catCols):
    for x in catCols:
        print(x)
        df_temp[x] = df_temp[x].astype('category')
        
    print(df_temp.dtypes)
    return
# ========================================================================================================================================
def read_multisheet_excel(fullPath,sheet):
    print("Multi-sheet xlsx, sheet: ",sheet)
    df_temp = pd.read_excel(fullPath,sheet)
    df_temp['sheet_name'] = sheet
    df_temp.columns = df_temp.columns.str.strip().str.lower().str.replace(' ', '_')
    print("sheet ",sheet,": ",df_temp.shape)    
    return df_temp
# ========================================================================================================================================
def read_archive(fullPath):
    print(fullPath)
    if (fullPath[-3:] == 'csv'):
        df_temp = pd.read_csv(fullPath,encoding='utf-8')
        df_temp.columns = df_temp.columns.str.strip().str.lower().str.replace(' ', '_')
        print(df_temp.shape)
        return df_temp
    
    if (fullPath[-7:] == 'parquet'):
        df_temp = pd.read_parquet(fullPath,engine='pyarrow')
        df_temp.columns = df_temp.columns.str.strip().str.lower().str.replace(' ', '_')
        print(df_temp.shape)
        return df_temp
        
    if (fullPath[-4:] == 'xlsx'):
        df_temp = pd.read_excel(fullPath)
        df_temp.columns = df_temp.columns.str.strip().str.lower().str.replace(' ', '_')
        print(df_temp.shape)
        return df_temp
    
    if (fullPath[-3:] == 'xls'):
        df_temp = pd.read_excel(fullPath)
        df_temp.columns = df_temp.columns.str.strip().str.lower().str.replace(' ', '_')
        print(df_temp.shape)
        return df_temp    
    
    return None
# ========================================================================================================================================
def read_csv_ascii(fullPath,error_stop):
    print(fullPath)
    if (fullPath[-3:] == 'csv'):
        print("csv")
        df_temp = pd.read_csv(fullPath,encoding = 'ISO-8859-1',error_bad_lines=error_stop)
        df_temp.columns = df_temp.columns.str.strip().str.lower().str.replace(' ', '_')
        print(df_temp.shape)
        return df_temp

    return None
# ================================================================================================================
def save_to_file(df_temp,filename):
    int_trim = None
    if(filename[-3:] == 'csv'):
        print("to csv")
        df_temp.columns = df_temp.columns.str.lower().str.replace(' ', '_')
        df_temp.to_csv(filename,index=False)
        
    if(filename[-7:] == 'parquet'):
        print("to parquet")
        df_temp.columns = df_temp.columns.str.lower().str.replace(' ', '_')
        df_temp.to_parquet(filename,engine='pyarrow',compression=None)

    if(filename[-8:] == 'parquet2'):
        print("to parquet v2")
        df_temp.columns = df_temp.columns.str.lower().str.replace(' ', '_')
        df_temp.to_parquet(filename[:-1],engine='pyarrow',compression='snappy',version='2.0')
        int_trim = -1
        
    if(filename[-4:] == 'xlsx'):
        print("to xlsx with engine xlsxwriter")
        df_temp.columns = df_temp.columns.str.lower().str.replace(' ', '_')
        writer = pd.ExcelWriter(filename,date_format='YYYY-MM-DD',datetime_format='YYYY-MM-DD', engine='xlsxwriter')
        df_temp.to_excel(writer,index=False)
        writer.save()
        
    if(filename[-5:] == 'xlsx2'):
        print("to xlsx with engine openpyxl")
        df_temp.columns = df_temp.columns.str.lower().str.replace(' ', '_')
        writer = pd.ExcelWriter(filename[:-1],date_format='YYYY-MM-DD',datetime_format='YYYY-MM-DD', engine='openpyxl')
        df_temp.to_excel(writer,index=False)
        writer.save()
        int_trim = -1
        
    print("DF ",df_temp.shape)
    print("Saved to "+str(filename[:int_trim]))
    return
# ================================================================================================================
def save_db_csv(df_temp,filename):
    import csv
    df_temp.columns = df_temp.columns.str.lower().str.replace(' ', '_')
    df_temp.to_csv(filename,quoting=csv.QUOTE_NONNUMERIC,index=False)
    print("Saved to "+str(filename))
    return
# ================================================================================================================
def duplicate_check(df_temp,subset_key,unique_key,headrow):
    print("Duplicated "+str(subset_key)+": ",df_temp[df_temp.duplicated(subset=subset_key,keep=False)].shape)
    print("Unique "+unique_key+": ",len(df_temp[df_temp.duplicated(subset=subset_key,keep=False)][unique_key].unique()))
    return df_temp[df_temp.duplicated(subset=subset_key,keep=False)].sort_values(by=unique_key).head(headrow)
# ================================================================================================================
def drop_duplicate(df_temp,subset_key):
    print(df_temp.shape)
    df_temp.drop_duplicates(subset=subset_key,inplace=True)
    print(df_temp.shape)
    print("Duplicated "+str(subset_key)+": ",df_temp[df_temp.duplicated(subset=subset_key,keep=False)].shape)
    return
# ================================================================================================================
def pd_merge(df_left,df_right,merge_how,left_key,right_key):   
    print("Left DF: ",df_left.shape)
    print("Right DF: ",df_right.shape)
    
    if (type(left_key) == 'str'):
        print("left unique: ",df_left[left_key].is_unique)
        print("right unique: ",df_right[left_key].is_unique)
    
    if (right_key == None):
        print("Single key join")
        df_temp = pd.merge(df_left,df_right,how=merge_how,on=left_key)
    else:
        print("Multiple / diff name key join")
        df_temp = pd.merge(df_left,df_right,how=merge_how,left_on=left_key,right_on=right_key)
        
    print("Merge before dedup: ",df_temp.shape)
    df_temp.drop_duplicates(keep='first',inplace=True)
    print("Merge final: ",df_temp.shape)
    return df_temp
# ================================================================================================================
def na_bk_to_empty(df_temp,colName,src_val,target_val):
    df_temp.loc[df_temp[colName] == src_val,colName] = target_val
    return
# ================================================================================================================
def loop_csv(csv_path,batch_id,df_concat,src_col,isHeader,isFooter,error_stop):
    
    df_temp = pd.read_csv(csv_path,header=isHeader,skipfooter=isFooter,error_bad_lines=error_stop)
    #print(csv_path[-12:-6]+" | "+str(df_temp.shape))
    df_temp.loc[:,src_col] = batch_id
    df_temp.columns = df_temp.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('/', '_')

    return pd.concat([df_temp,df_concat], ignore_index=True, sort=False)    
# ================================================================================================================
def loop_coupon_csv(csv_path,df_concat,src_col,isHeader,isFooter):
    
    df_temp = pd.read_csv(csv_path,header=isHeader,skipfooter=isFooter)
    print(csv_path[-12:-6]+" | "+str(df_temp.shape))
    df_temp.loc[:,src_col] = csv_path[-12:-6]
    df_temp.columns = df_temp.columns.str.strip().str.lower().str.replace(' ', '_')

    return pd.concat([df_temp,df_concat], ignore_index=True, sort=False)    
# ================================================================================================================
def redeemed_coupon(mthList):
    df_redem = pd.DataFrame()
    
    for x in mthList:
        df_redem = loop_coupon_csv(r"\\10.1.106.217\AutoSmart\Retail 15\Report Record\Digital Coupon Report\Coupon redeemed report\DigitCouponRedeem_"+str(x)+".csv",df_redem,'redeem_mth',1,1)
        print("Concated: "+str(df_redem.shape))
        
    print("Final: "+str(df_redem.shape))
    return df_redem
# ================================================================================================================
def issued_coupon(mthList):
    df_issue = pd.DataFrame()
    
    for x in mthList:
        df_issue = loop_coupon_csv(r"\\10.1.106.217\AutoSmart\Retail 15\Report Record\Digital Coupon Report\Digital Coupon Assignment Report\DigitCouponAssign_"+str(x)+".csv",df_issue,'issue_mth',1,1)
        print("Concated: "+str(df_issue.shape))
        
    print("Final: "+str(df_issue.shape))
    return df_issue
# ================================================================================================================
def unstack_by_key(df_temp,key_col):
    df_out = df_temp.set_index([key_col,df_temp.groupby(key_col).cumcount() + 1]).unstack()
    df_out.columns = df_out.columns.map('{0[0]}_{0[1]}'.format)
    df_out = df_out.reset_index()
    print("Key col is unique: ",df_temp[key_col].is_unique)
    print("Final shape: ",df_out.shape)
    return df_out
# ================================================================================================================
def set_channel_grp(df_tmp,channel_col,shopcode_col):
    print("Unique channel: ",df_tmp[channel_col].unique())
    
    df_tmp.loc[df_tmp[channel_col].isin(["1010","csl.","csl. Shop","1010 Centre","Q Shop","HKT Shop"]),channel_col+'_grp'] = "Retail"
    
    df_tmp.loc[df_tmp[channel_col] == "DIRECT SALES",channel_col+'_grp'] = "Roadshow"
    df_tmp.loc[(df_tmp[channel_col].notna()) & (df_tmp[channel_col].str.contains("Roadshow",case=False)),channel_col+'_grp'] = "Roadshow"
    df_tmp.loc[df_tmp[channel_col].isin(["CALL CENTER","HKT CALL CENTER - ","O2F (Outbound Telesales)"]),channel_col+'_grp'] = "CCS"
    df_tmp.loc[(df_tmp[channel_col].notna()) & (df_tmp[channel_col].str.contains("CALL CENTER",case=False)),channel_col+'_grp'] = "CCS"
    df_tmp.loc[(df_tmp[channel_col].notna()) & (df_tmp[channel_col].str.contains("Call Centre",case=False)),channel_col+'_grp'] = "CCS"
    df_tmp.loc[(df_tmp[channel_col].notna()) & (df_tmp[channel_col].str.contains("TELESALES",case=False)),channel_col+'_grp'] = "CCS"    
    df_tmp.loc[df_tmp[channel_col] == "HKT CALL CENTER - CSO",channel_col+'_grp'] = "CRM"
    
    df_tmp[channel_col+'_grp'].fillna("Others",inplace=True)

    df_tmp.reset_index(drop=True,inplace=True)
    
    df_tmp.loc[df_tmp[channel_col].isin(["1010","csl.","HKT","csl. Online"]),shopcode_col] = df_tmp[shopcode_col].str[-3:]
    
    print(df_tmp[channel_col+'_grp'].value_counts(dropna=False))
    return
# ================================================================================================================
def trim_shop_code(df_tmp,shopcode_col):
    df_tmp.loc[(df_tmp[shopcode_col].str.len() == 4) & (df_tmp[shopcode_col].str[0] == "P"),shopcode_col] = df_tmp[shopcode_col].str[-3:]
    print("Trimmed ",shopcode_col)
    return
# ================================================================================================================
def set_date_to_month(df_tmp,datetime_col,desire_col):
    df_tmp.loc[:,desire_col] = df_tmp[datetime_col].dt.strftime("%Y%m")
    
    print(df_tmp[desire_col].value_counts(dropna=False).head())
    return
# ================================================================================================================
def set_yyyymm_to_quarter(df_tmp,datetime_col,desire_col):
    df_tmp.loc[(df_tmp[datetime_col].notna()) & (df_tmp[datetime_col].str[-2:] == "01"),desire_col] = df_tmp[datetime_col].str[:4]+"Q1"
    df_tmp.loc[(df_tmp[datetime_col].notna()) & (df_tmp[datetime_col].str[-2:] == "02"),desire_col] = df_tmp[datetime_col].str[:4]+"Q1"
    df_tmp.loc[(df_tmp[datetime_col].notna()) & (df_tmp[datetime_col].str[-2:] == "03"),desire_col] = df_tmp[datetime_col].str[:4]+"Q1"
    df_tmp.loc[(df_tmp[datetime_col].notna()) & (df_tmp[datetime_col].str[-2:] == "04"),desire_col] = df_tmp[datetime_col].str[:4]+"Q2"
    df_tmp.loc[(df_tmp[datetime_col].notna()) & (df_tmp[datetime_col].str[-2:] == "05"),desire_col] = df_tmp[datetime_col].str[:4]+"Q2"
    df_tmp.loc[(df_tmp[datetime_col].notna()) & (df_tmp[datetime_col].str[-2:] == "06"),desire_col] = df_tmp[datetime_col].str[:4]+"Q2"
    df_tmp.loc[(df_tmp[datetime_col].notna()) & (df_tmp[datetime_col].str[-2:] == "07"),desire_col] = df_tmp[datetime_col].str[:4]+"Q3"
    df_tmp.loc[(df_tmp[datetime_col].notna()) & (df_tmp[datetime_col].str[-2:] == "08"),desire_col] = df_tmp[datetime_col].str[:4]+"Q3"
    df_tmp.loc[(df_tmp[datetime_col].notna()) & (df_tmp[datetime_col].str[-2:] == "09"),desire_col] = df_tmp[datetime_col].str[:4]+"Q3"
    df_tmp.loc[(df_tmp[datetime_col].notna()) & (df_tmp[datetime_col].str[-2:] == "10"),desire_col] = df_tmp[datetime_col].str[:4]+"Q4"
    df_tmp.loc[(df_tmp[datetime_col].notna()) & (df_tmp[datetime_col].str[-2:] == "11"),desire_col] = df_tmp[datetime_col].str[:4]+"Q4"
    df_tmp.loc[(df_tmp[datetime_col].notna()) & (df_tmp[datetime_col].str[-2:] == "12"),desire_col] = df_tmp[datetime_col].str[:4]+"Q4"
    return
# ================================================================================================================
def dask_dtypes(df_temp,dateCols,int64Cols,intCols,date_format):
    for x in dateCols:
        print(x)
        df_temp[x] = df_temp[x].fillna('1900-01-01')
        df_temp[x] = dd.to_datetime(df_temp[x],errors='coerce',yearfirst=True)

    for x in int64Cols:
        print(x)
        df_temp[x] = df_temp[x].fillna('')
        df_temp[x] = df_temp[x].astype('int64')    
    
    for x in intCols:
        print(x)
        df_temp[x] = df_temp[x].fillna('')
        df_temp[x] = df_temp[x].astype(int)   

    print(df_temp.dtypes)
    return df_temp
# ================================================================================================================
def sm_num_to_date_mth(df_temp,sm_num_cols,create_date_col,create_mth_col):
    df_temp.loc[df_temp[sm_num_cols].str[0]=='0',create_date_col] = "2020"+df_temp[sm_num_cols].str[1:5]
    df_temp.loc[df_temp[sm_num_cols].str[0]=='1',create_date_col] = "2021"+df_temp[sm_num_cols].str[1:5]
    df_temp.loc[df_temp[sm_num_cols].str[0]=='9',create_date_col] = "2019"+df_temp[sm_num_cols].str[1:5]
    df_temp.loc[df_temp[sm_num_cols].str[0]=='8',create_date_col] = "2018"+df_temp[sm_num_cols].str[1:5]
    df_temp.loc[df_temp[sm_num_cols].str[0]=='7',create_date_col] = "2017"+df_temp[sm_num_cols].str[1:5]
    df_temp.loc[df_temp[sm_num_cols].str[0]=='6',create_date_col] = "2016"+df_temp[sm_num_cols].str[1:5]
    df_temp.loc[df_temp[sm_num_cols].str[0]=='5',create_date_col] = "2015"+df_temp[sm_num_cols].str[1:5]
    df_temp.loc[df_temp[sm_num_cols].str[0]=='4',create_date_col] = "2014"+df_temp[sm_num_cols].str[1:5]
    df_temp.loc[df_temp[sm_num_cols].str[0]=='3',create_date_col] = "2013"+df_temp[sm_num_cols].str[1:5]
    df_temp.loc[df_temp[sm_num_cols].str[0]=='2',create_date_col] = "2012"+df_temp[sm_num_cols].str[1:5]
    
    df_temp[create_date_col] = pd.to_datetime(df_temp[create_date_col],format="%Y%m%d")
    
    set_date_to_month(df_temp,create_date_col,create_mth_col)
    
    return
# ================================================================================================================
def list_to_sql_list(arr,is_str):
    if (is_str == True):
        print("Turning into String List, length:",len(arr))
        str_list = [str(x) for x in arr]
        str_list_sql = str(str_list).strip('[]')
        print("Turned into String List")
        return str_list_sql
    else:
        print("Turning into Int List, length:",len(arr))
        str_list = [str(x) for x in arr]
        str_list_sql = str(str_list).strip('[]').replace("'","")
        print("Turned into Int List")
        return str_list_sql    
    return
# ================================================================================================================
def df_col_rename(df_tmp,arr):
    df_tmp.rename(arr,axis=1,inplace=True)
    return
# ================================================================================================================
def df_drop_col(df_tmp,arr):
    df_tmp.drop(arr,axis=1,inplace=True)
    return
# ================================================================================================================
def df_col_format(df_tmp):
    df_tmp.columns = df_tmp.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('/', '_')
    return
# ================================================================================================================
def df_drop_duplicates(df_tmp,col_unique,keep_method):
    print("Unique columns:",col_unique," keep_method:",keep_method)
    print("Before:",df_tmp.shape)
    df_tmp.drop_duplicates(subset=col_unique,keep=keep_method,inplace=True)
    print("After:",df_tmp.shape)
    return
# ================================================================================================================
# import warnings
# warnings.filterwarnings("ignore")

def shop_master_mapping():
    file = r"\\cslfs02\Project02\RetailSales\Report\Mapping\Master Mapping Table.xlsx"

    df_mapping = pd.read_excel(file,"Mapping",header=1)

    df_tmp = df_mapping.iloc[:,[4,6,7,12]]
    df_tmp.columns = ["shop_code","Channel","Team","SM"]

    df_tmp.drop_duplicates(subset="shop_code",inplace=True,keep='first')

    df_tmp.loc[df_tmp['Channel'].isin(["1010 Centre","csl. Shop","HKT"]),'Channel_grp'] = "Retail"
    df_tmp.loc[df_tmp['Channel'].isin(["Call Centre"]),'Channel_grp'] = "CCS"
    df_tmp.loc[df_tmp['Channel'].isin(["Roadshow"]),'Channel_grp'] = "Roadshow"
    df_tmp.loc[df_tmp['Channel'].isin(["HKT Shop"]),'Channel_grp'] = "Retail"
    df_tmp.loc[df_tmp['shop_code'] == "CRM",'Channel_grp'] = "CRM"
    df_tmp.loc[df_tmp['shop_code'] == "CRT",'Channel_grp'] = "CCS"
    df_tmp.loc[df_tmp['shop_code'] == "ORT",'Channel_grp'] = "CCS"

    df_tmp = df_tmp.append({'shop_code':"CRT",'Channel':"CCS",'Channel_grp':"CCS"},ignore_index=True)
    df_tmp = df_tmp.append({'shop_code':"ORT",'Channel':"CCS",'Channel_grp':"CCS"},ignore_index=True)

    df_col_format(df_tmp)
    
    print("Final:",df_tmp.shape)
    return df_tmp
# ================================================================================================================
# Two dates diff in months
def get_months_diff(df_tmp,col_start_date,col_end_date,col_new_period_col,int_float):
    df_tmp[col_new_period_col] = (df_tmp[col_end_date] - df_tmp[col_start_date]) / np.timedelta64(1,'M')
    df_tmp[col_new_period_col] = df_tmp[col_new_period_col].astype(int_float)
    return
# ================================================================================================================
# Get end date by start date and month interval
def get_enddate_bymonth(df_tmp,col_start_date,col_expiry_period,col_new_period_col):
    df_tmp[col_new_period_col] = df_tmp[col_start_date] + (df_tmp[col_expiry_period] * np.timedelta64(1,'M'))
    df_tmp[col_new_period_col] = pd.to_datetime(df_tmp[col_new_period_col]).dt.date
    return
# ================================================================================================================
def contact_num_clean(df_tmp,field):
    df_tmp.loc[df_tmp[field].str.len() != 8,field] = np.nan
    print("1: ",df_tmp[field].notna().sum())
    df_tmp.loc[df_tmp[field].isin(['99999999','00000000','88888888']),field] = np.nan
    print("2: ",df_tmp[field].notna().sum())
    df_tmp.loc[df_tmp[field].str.isnumeric() == False,field] = np.nan
    print("3: ",df_tmp[field].notna().sum())
    df_tmp.loc[~df_tmp[field].map(str).str[0].isin(['9','7','6','5','4']),field] = np.nan
    print("Final: ",df_tmp[field].notna().sum())
    return

# ================================================================================================================
def cust_subr_remove_dup_mobnum(df_tmp):
    df_tmp.loc[(df_tmp.duplicated(subset='mob_num',keep=False)) & (df_tmp.subr_stat_desc == "Postpaid Deactive") & (df_tmp.mob_num > 1),'remove'] = 'x'

    print("Before: ",df_tmp.shape)
    df_tmp = df_tmp[df_tmp.remove != 'x']
    print("After: ",df_tmp.shape)

    print("MRT is unique:",df_tmp.mob_num.is_unique)
    df_drop_col(df_tmp,['remove'])
    return
# ================================================================================================================
def lookup_prod_desc(df_tmp,df_map,left_col,right_col,drop_prod_name,map_desc_colname,prod_desc_colname):
    print(df_tmp.shape)
    df_tmp = df_tmp.merge(df_map,left_on=left_col,right_on=right_col,how='left')
    print(df_tmp.shape)
    
    if drop_prod_name == True:
        df_drop_col(df_tmp,[left_col,right_col])
        
    df_col_rename(df_tmp,{map_desc_colname:prod_desc_colname})
    return df_tmp
# ================================================================================================================
def edw_bulk_query_singlekey(str_tmp_table_spec,str_tmp_key,str_key_datatype,arr_key,sql_query):
    insert_count = 0
    conn = get_edw_conn_obj()
    print("START: "+str(datetime.datetime.now()))
    df_EDW_Query = pd.DataFrame()
    
    try:
        cuxr = conn.cursor()
        print("Create volatile table: ",cuxr.execute("create volatile table testing ("+str_tmp_table_spec+") on commit preserve rows"))
        
        if str_key_datatype == "Integer":
            print("Insert Integer")
            for x in arr_key:
                cuxr.execute("insert into testing ("+str(x)+");")
                insert_count = insert_count + 1
        else:
            print("Insert String")
            for x in arr_key:
                cuxr.execute("insert into testing ('"+x+"');")
                insert_count = insert_count + 1            

        print("Total inserted",insert_count,str_tmp_key," at ",str(datetime.datetime.now()))
                
        df_EDW_Query = pd.read_sql(sql_query,conn)
        df_col_format(df_EDW_Query)
        print(df_EDW_Query.shape)
        print("Complete: "+str(datetime.datetime.now()))
        
    except tsql.OperationalError as e:
        print("EDW OperationalError: "+str(e))
    except tsql.DatabaseError as e:
        print("EDW DatabaseError: "+str(e))
    except tsql.Error as e:
        print("EDW Error: "+str(e))     
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error")
    finally:
        conn.close()
        print("Conn closed: "+str(datetime.datetime.now()))

    return df_EDW_Query
# ================================================================================================================
def sm_num_to_pos_sys(df_temp,sm_num_col):
    df_temp.loc[df_temp[sm_num_col].str[8:10]=="SB",'pos_sys_src'] = "SBPOS"
    df_temp['pos_sys_src'] = df_temp['pos_sys_src'].fillna('POS')
    return 
# ================================================================================================================
def sm_num_to_shop(df_temp,sm_num_col):
    df_temp.loc[:,'sm_shop'] = df_temp[sm_num_col].str[5:8]
    df_temp['sm_shop'] = df_temp['sm_shop'].fillna('NA')
    return 