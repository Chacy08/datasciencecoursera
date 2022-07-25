import pandas as pd
import pyodbc as podbc
import subprocess
import datetime
import csv

import configparser
config = configparser.RawConfigParser()
config.read(r'D:\Coding\MSSQL_project\MOB_SALES_MSSQL\AutoJob\mssql_account.ini')

server_local_win = config['Variable']['server_local_win']
server_prod_sql = config['Variable']['server_prod_sql']

fixed_tmp_path = config['Path']['fixed_tmp_path']
fixed_tmp_sqlcmd_log = config['Path']['fixed_tmp_sqlcmd_log']

db_mstr_user = config['Account']['db_mstr_user']
db_mstr_pw = config['Account']['db_mstr_pw']

db_creator_user = config['Account']['db_creator_user']
db_creator_pw = config['Account']['db_creator_pw']

db_dev_user = config['Account']['db_dev_user']
db_dev_pw = config['Account']['db_dev_pw']

db_expl_user = config['Account']['db_expl_user']
db_expl_pw = config['Account']['db_expl_pw']

db_view_user = config['Account']['db_view_user']
db_view_pw = config['Account']['db_view_pw']

db_msts_user = config['Account']['db_msts_user']
db_msts_pw = config['Account']['db_msts_pw']

db_msts_app_user = config['Account']['db_msts_app_user']
db_msts_app_pw = config['Account']['db_msts_app_pw']

db_vis = config['Variable']['db_vis']
db_analy = config['Variable']['db_analy']
db_dev = config['Variable']['db_dev']
db_inf = config['Variable']['db_inf']
db_msa_mstr = config['Variable']['db_msa_mstr']

str_dev_tmp_table = config['Variable']['str_dev_tmp_table']

# ========================================================================================================================================
def get_usrname_pwd(str_db_acc):
    if str_db_acc == "ms_creator":
        username = db_creator_user
        password = db_creator_pw
    elif str_db_acc == "ms_viewer":
        username = db_view_user
        password = db_view_pw
    elif str_db_acc == "ms_explorer":
        username = db_expl_user
        password = db_expl_pw
    elif str_db_acc == "ms_developer":
        username = db_dev_user
        password = db_dev_pw
    elif str_db_acc == "ms_master":
        username = db_mstr_user
        password = db_mstr_pw
    else:
        print("DB Account NA")
        return 0,0

    return username,password

#========================================================================================================================================
def get_mssql_conn_sql_auth(str_db,str_db_acc,bool_autocommit):

    username,password = get_usrname_pwd(str_db_acc)
    return podbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=tcp:'+server_prod_sql+';Database='+str_db+';UID='+username+';PWD='+password+';',autocommit=bool_autocommit)

def get_mssql_conn_win_auth(str_db):
    return podbc.connect('Driver={ODBC Driver 17 for SQL Server};Server='+server_local_win+';Database='+str_db+';Trusted_Connection=yes;')
# ========================================================================================================================================
def mssql_query(sql,str_db,str_db_acc):
    cnxn = get_mssql_conn_sql_auth(str_db,str_db_acc,False)
    print("SQL Auth, Connected to:",str_db," | User:",str_db_acc," | Start:",str(datetime.datetime.now()))
    try:
        df_MS_Query = pd.DataFrame()
        df_MS_Query = pd.read_sql(sql,cnxn)
        df_MS_Query.columns = df_MS_Query.columns.str.strip().str.lower().str.replace(' ', '_')
        print(df_MS_Query.shape)
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error:",sys.exc_info())
    finally:      
        cnxn.close()
    
    print("END: "+str(datetime.datetime.now()))
    return df_MS_Query
# ========================================================================================================================================
def mssql_execute(sql,str_db,str_db_acc):
    cnxn = get_mssql_conn_sql_auth(str_db,str_db_acc,True)
    print("SQL Auth, Connected to:",str_db," | User:",str_db_acc," | Start:",str(datetime.datetime.now()))
    str_exc_result = None
    try:
        cursor = cnxn.cursor()
        cursor.execute(sql)
        cursor.commit()
        print("SQL Executed",cursor.messages)
        str_exc_result = cursor.messages
        cursor.close()
    except podbc.Error as e:
        print("DB error",str(e))
        str_exc_result = str(e)
    except BaseException as e:
        print("Sys error: ",str(e))
        str_exc_result = str(e)
    except:
        print("General Error:",sys.exc_info())
        str_exc_result = str(sys.exc_info())
    finally:      
        cnxn.close()
    
    print("END: "+str(datetime.datetime.now()))
    return str_exc_result
# ========================================================================================================================================
def mssql_execute_batch(arr_sql,str_db,str_db_acc):
    cnxn = get_mssql_conn_sql_auth(str_db,str_db_acc,False)
    print("SQL Auth, Connected to:",str_db," | User:",str_db_acc," | Start:",str(datetime.datetime.now()))
    str_exc_result = None
    try:
        cursor = cnxn.cursor()
        
        for x in arr_sql:
            try:
                cursor.execute(x)
            except podbc.Error as e:
                print("Cursor: |",str(e))
                str_exc_result = str(e)
                cnxn.rollback()
                break
            except BaseException as e:
                print("Cursor: |",str(e))
                str_exc_result = str(e)
                cnxn.rollback()
                break
            except:
                print("Cursor: |",sys.exc_info())
                str_exc_result = str(sys.exc_info())
                cnxn.rollback()
                break
                
        cursor.commit()
        print("SQL Executed")
        cursor.close()
    except podbc.Error as e:
        print("Conn: |",str(e))
        str_exc_result = str(e)
    except BaseException as e:
        print("Conn: |",str(e))
        str_exc_result = str(e)
    except:
        print("Conn: |",sys.exc_info())
        str_exc_result = str(sys.exc_info())
    finally:      
        cnxn.close()
    
    print("END: "+str(datetime.datetime.now()))
    return str_exc_result
# ========================================================================================================================================
def mssql_execute_autocommit(sql,str_db,str_db_acc):
    cnxn = get_mssql_conn_sql_auth(str_db,str_db_acc,True)
    print("SQL Auth, Connected to:",str_db," | User:",str_db_acc," | Start:",str(datetime.datetime.now()))
    str_exc_result = None
    try:
        cursor = cnxn.cursor()
        cursor.execute(sql)
        cursor.commit()
        print("SQL Executed")
        cursor.close()
    except podbc.Error as e:
        print("DB error",str(e))
        str_exc_result = str(e)
    except BaseException as e:
        print("Sys error: ",str(e))
        str_exc_result = str(e)
    except:
        print("General Error:",sys.exc_info())
        str_exc_result = str(sys.exc_info())
    finally:      
        cnxn.close()
    
    print("END: "+str(datetime.datetime.now()))
    return str_exc_result
# ========================================================================================================================================
def save_tmp_csv(df_tmp):
    df_tmp.columns = df_tmp.columns.str.lower().str.replace(' ', '_')
    df_tmp.to_csv(fixed_tmp_path,index=False,header=False)
    print("tmp DF:",df_tmp.shape,"Saved tmp:",fixed_tmp_path)
    return
# ========================================================================================================================================
def mssql_bulk_insert_copy_exists_table(df_src,str_exist_table,str_pk,str_constraint,sql_insert,str_db,str_db_acc):
    '''
    1. replicate tmp table (permanent) from target table in default name
    2. Dataframe to tmp CSV
    3. bcp in into tmp table
    4. insert into target table from tmp table, by custom insert sql
    5. drop tmp table (permanent)
    '''

    obj_conn = get_mssql_conn_sql_auth(str_db,str_db_acc,False)
    print("Connected by",str_db_acc)
    
    cursor = obj_conn.cursor()
    
    try:
        cursor.execute("drop table if exists "+str_dev_tmp_table+";")
        cursor.commit()
        
        cursor.execute("select * into "+str_dev_tmp_table+" from "+str_exist_table+" where 1 = 0;")
        cursor.commit()
        print("Tmp table created",str_dev_tmp_table)
        
        if str_pk != None:
            cursor.execute("alter table "+str_dev_tmp_table+" ADD CONSTRAINT PK_F_Tmp PRIMARY KEY ("+str_pk+");")
            cursor.commit()
            print("Tmp table created PK",str_pk)
        
        bool_tmp_loaded = False
        
        bool_tmp_loaded = bcp_in_exists_table(df_src,str_dev_tmp_table,str_db_acc)
        print("Loaded tmp table",bool_tmp_loaded)
        
        if bool_tmp_loaded == True:
            print("Insert data")
            cursor.execute(sql_insert)
            cursor.commit()    
            print("Insert done")
            
        
        cursor.execute("drop table if exists "+str_dev_tmp_table+";")
        cursor.commit()  
        print("Query Done")        
        
        
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error:",sys.exc_info())
    finally:
        cursor.execute("drop table if exists "+str_dev_tmp_table+";")
        cursor.commit()
        print("Tmp table dropped")
        obj_conn.close()
        
        
    print("Bulk insert done")
    return

# ========================================================================================================================================
def mssql_bulk_insert_table_sql(df_src,str_tmp_table,tmp_table_sql,sql_insert,str_constraint,str_db,str_db_acc):

    '''
    1. Dataframe to tmp CSV
    2. create tmp table (permanent) on db.schema.table with custom column + data type + constraint
    3. bcp in into tmp table
    4. insert into target table from tmp table, by custom insert sql
    5. drop tmp table (permanent)
    '''
    save_tmp_csv(df_src)

    obj_conn = get_mssql_conn_sql_auth(str_db,str_db_acc,False)
    print("Connected by",str_db_acc)
    
    cursor = obj_conn.cursor()
    
    try:
        cursor.execute("drop table if exists "+str_tmp_table+";")
        cursor.commit()
        
        cursor.execute("create table "+str_tmp_table+"("+tmp_table_sql+");")
        cursor.commit()    
        print("Tmp table created",str_tmp_table)
        
        bool_tmp_loaded = False
        
        bool_tmp_loaded = bcp_in_exists_table(df_src,str_tmp_table,str_db_acc)
        print("Loaded tmp table",bool_tmp_loaded)
        
        if bool_tmp_loaded == True:
            print("Insert data")
            cursor.execute(sql_insert)
            cursor.commit()    
            print("Insert done")
            
        
        cursor.execute("drop table if exists "+str_tmp_table+";")
        cursor.commit()  
        print("Query Done")        
        
        
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error:",sys.exc_info())
    finally:
        cursor.execute("drop table if exists "+str_tmp_table+";")
        cursor.commit()
        print("Tmp table dropped")
        obj_conn.close()

    print("Bulk insert done")
    return
# ========================================================================================================================================
def mssql_to_null(str_exist_table,arr_col,str_from,str_db):
    
    for x in arr_col:
        sql = "update "+str_exist_table+" set "+x+" = null where "+x+" = "+str_from
        print(sql)
        mssql_execute(sql,str_db)
    
    return
# ========================================================================================================================================
def column_str_replace(df_tmp,col_name):
    df_tmp[col_name] = df_tmp[col_name].str.replace('"', '', regex=False)
    df_tmp[col_name] = df_tmp[col_name].str.replace("'", "", regex=False)
    df_tmp[col_name] = df_tmp[col_name].str.replace("\t", "", regex=False)
    df_tmp[col_name] = df_tmp[col_name].str.replace("\\", "", regex=False)
    
    df_tmp[col_name] = df_tmp[col_name].str.replace(",", " ", regex=False)
    df_tmp[col_name] = df_tmp[col_name].str.replace("\n", " ", regex=False)
    df_tmp[col_name] = df_tmp[col_name].str.replace("\r", " ", regex=False)
    return

#========================================================================================================================================
def bcp_in_exists_table(df_tmp,str_tar_table,str_db_acc):
    
    try:
        save_tmp_csv(df_tmp)
        
        username,password = get_usrname_pwd(str_db_acc)
        
        process_result = None
        process_success = False
        
        cmd = r'bcp '+str_tar_table+' in "'+fixed_tmp_path+'" -S '+server_prod_sql+' -c -t, -F1 -r\r\n -U "'+username+'" -P "'+password+'"'
        #print(cmd)

        process_result = subprocess.run(cmd, stdout=subprocess.PIPE)
        process_success = True
        print("CMD Executed ",process_result)
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error:",sys.exc_info())
    finally:      
        print("CMD Ended")
        
    print("END: "+str(datetime.datetime.now()))
    return process_success
#========================================================================================================================================
def bcp_in_exists_table_custom(str_csv_path,str_tar_table,int_header_row,str_breakline_symbol,str_db_acc): 

    try:       
        username,password = get_usrname_pwd(str_db_acc)
        
        process_result = None
        process_success = False
        
        cmd = r'bcp '+str_tar_table+' in "'+str_csv_path+'" -S '+server_prod_sql+' -c -t, -F '+str(int_header_row)+' -r '+str_breakline_symbol+' -U "'+username+'" -P "'+password+'"'
        #print(cmd)

        process_result = subprocess.run(cmd, stdout=subprocess.PIPE)
        process_success = True
        print("CMD Executed ",process_result)
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error:",sys.exc_info())
    finally:      
        print("CMD Ended")
        
    print("END: "+str(datetime.datetime.now()))
    return process_success
#========================================================================================================================================
def mssql_bulk_query(df_tmp,tmp_table_sql,arr_constraint,sql_query,str_db,str_db_acc):

    '''
    1. Dataframe to tmp CSV
    2. create tmp table (permanent) on db.schema.table with custom column + data type + constraint
    3. bcp in into tmp table
    4. query target table from tmp table, by custom query sql
    5. drop tmp table (permanent)
    '''
    save_tmp_csv(df_tmp)

    obj_conn = get_mssql_conn_sql_auth(str_db,str_db_acc,False)
    print("Connected by",str_db_acc)
    
    cursor = obj_conn.cursor()

    df_MS_Query = pd.DataFrame()
    
    try:       
        username,password = get_usrname_pwd(str_db_acc)

        # Create tmp table
        
        cursor.execute("drop table if exists "+str_dev_tmp_table+";")
        cursor.commit()
        
        cursor.execute("create table "+str_dev_tmp_table+"("+tmp_table_sql+");")
        cursor.commit()    
        print("Tmp table created",str_dev_tmp_table)
        
        # Add tmp table constraint
        
        
        # Load data into tmp table
        bool_tmp_loaded = False
        
        bool_tmp_loaded = bcp_in_exists_table_custom(fixed_tmp_path,str_dev_tmp_table,1,'\r\n',str_db_acc)
        print("Loaded tmp table",bool_tmp_loaded)
        
        if bool_tmp_loaded == True:
            print("Query with tmp data")
            df_MS_Query = pd.read_sql(sql_query,obj_conn)
            df_MS_Query.columns = df_MS_Query.columns.str.strip().str.lower().str.replace(' ', '_')
            print("Result:",df_MS_Query.shape)

        cursor.execute("drop table if exists "+str_dev_tmp_table+";")
        cursor.commit()
        print("Tmp table dropped",str_dev_tmp_table)

    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error:",sys.exc_info())
    finally:      
        print("CMD Ended")
        obj_conn.close()

    print("END: "+str(datetime.datetime.now()))
    return df_MS_Query

#========================================================================================================================================
def mssql_bulk_update(df_tmp,tmp_table_sql,arr_constraint,sql_execute,str_db,str_db_acc):

    '''
    1. Dataframe to tmp CSV
    2. create tmp table (permanent) on db.schema.table with custom column + data type + constraint
    3. bcp in into tmp table
    4. update target table from tmp table, by custom query sql
    5. drop tmp table (permanent)
    '''
    int_success = 0
    
    save_tmp_csv(df_tmp)

    obj_conn = get_mssql_conn_sql_auth(str_db,str_db_acc,False)
    print("Connected by",str_db_acc)
    
    cursor = obj_conn.cursor()

    df_MS_Query = pd.DataFrame()
    
    try:       
        username,password = get_usrname_pwd(str_db_acc)

        # Create tmp table
        
        cursor.execute("drop table if exists "+str_dev_tmp_table+";")
        cursor.commit()
        
        cursor.execute("create table "+str_dev_tmp_table+"("+tmp_table_sql+");")
        cursor.commit()    
        print("Tmp table created",str_dev_tmp_table)
        
        # Add tmp table constraint
        
        
        # Load data into tmp table
        bool_tmp_loaded = False
        
        bool_tmp_loaded = bcp_in_exists_table_custom(fixed_tmp_path,str_dev_tmp_table,1,'\r\n',str_db_acc)
        print("Loaded tmp table",bool_tmp_loaded)
        
        if bool_tmp_loaded == True:
            print("Execute update from tmp data")
            cursor.execute(sql_execute)
            cursor.commit()
            print("Executed update")
            int_success = 1

        cursor.execute("drop table if exists "+str_dev_tmp_table+";")
        cursor.commit()
        print("Tmp table dropped",str_dev_tmp_table)

    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error:",sys.exc_info())
    finally:      
        print("CMD Ended")
        obj_conn.close()

    print("END: "+str(datetime.datetime.now()))
    return int_success
#========================================================================================================================================
def execute_sql_script(sql_script_path,str_db_acc): 

    try:       
        username,password = get_usrname_pwd(str_db_acc)
        
        process_result = None
        process_success = False
        
        cmd = r'sqlcmd -S '+server_prod_sql+' -i "'+str(sql_script_path)+'" -o "'+str(fixed_tmp_sqlcmd_log)+'" -U "'+username+'" -P "'+password+'"'
        print(cmd)
        
        process_result = subprocess.run(cmd, stdout=subprocess.PIPE)
        process_success = True
        print("CMD Executed ",process_result)
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error:",sys.exc_info())
    finally:      
        print("CMD Ended")
        
    print("END: "+str(datetime.datetime.now()))
    return process_success
#========================================================================================================================================
def bcp_export(sql_query,str_table,str_output_path,str_db_acc):

    try:       
        username,password = get_usrname_pwd(str_db_acc)
        
        process_result = None
        process_success = False
        
        if sql_query == None and str_table != None:
            print("OUT mode")
            cmd = r'bcp '+str_table+' out "'+str_output_path+'" -S '+server_prod_sql+' -c -t, -U "'+username+'" -P "'+password+'"'
        elif sql_query != None and str_table == None:
            print("Query mode")
            cmd = r'bcp "'+sql_query+'" queryout "'+str_output_path+'" -S '+server_prod_sql+' -c -t, -U "'+username+'" -P "'+password+'"'
        else:
            print("!!! No mode selected !!!")
            return False
        
        process_result = subprocess.run(cmd, stdout=subprocess.PIPE)
        process_success = True
        print("CMD Executed ",process_result)
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error:",sys.exc_info())
    finally:      
        print("CMD Ended")
        
    print("END: "+str(datetime.datetime.now()))
    return process_success