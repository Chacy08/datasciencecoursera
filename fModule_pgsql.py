import psycopg2
import pandas as pd
import datetime

str_default_db_cluster = "12"
#------------------------------------------------------------------------------------------------------------------
def pgsql_get_connected_object(str_db_cluster):

    if str_db_cluster == "12":
        obj_conn = psycopg2.connect(dbname="tdm", user="fergend_crud", port=5423, password="Fergend_Crud@1625")
    elif str_db_cluster == "14":
        obj_conn = psycopg2.connect(dbname="tdm", user="fergend_crud", port=5413, password="Fergend_Crud@1625")
    else:
        obj_conn = psycopg2.connect(dbname="tdm", user="fergend_crud", port=5423, password="Fergend_Crud@1625")
    
    print("PG Cluster",str_db_cluster,"| PGSQL current encoding: ",obj_conn.encoding,str(datetime.datetime.now()))
    return obj_conn
#------------------------------------------------------------------------------------------------------------------
def dmart_date(str_db_cluster):
    return pgsql_query("select * from dmart_date",False,str_db_cluster)
#------------------------------------------------------------------------------------------------------------------
def pgsql_query(sql,bool_explain,str_db_cluster):
    # Connect to an existing database
    conn = pgsql_get_connected_object(str_db_cluster)    
    df_tmp = pd.DataFrame()
    #print("START: "+str(datetime.datetime.now()))

    try:
        if bool_explain == True:
            print("Explain Query")
            cur = conn.cursor()
            tur_tmp = None
            cur.execute("explain "+sql)
            tur_tmp = cur.fetchall()
            df_tmp = tur_tmp
        else:
            df_tmp = pd.read_sql(sql, conn)
            print(df_tmp.shape)
            
    except psycopg2.Error as e:
        print("** DB Exception occurs ** "+str(e.pgerror))
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("Error")
    finally:
        conn.close()
        
    print("END: "+str(datetime.datetime.now()))
    return df_tmp
#------------------------------------------------------------------------------------------------------------------
def pgsql_cur_query(sql,bool_explain,str_db_cluster):
    conn = pgsql_get_connected_object(str_db_cluster)
    cur = conn.cursor()
    tur_tmp = None
    try:
        if bool_explain == True:
            print("Explain Query")
            cur.execute("explain "+sql)
        else:
            cur.execute(sql)
            
        tur_tmp = cur.fetchall()
    except psycopg2.Error as e:
        print("** DB Exception occurs ** "+str(e.pgerror))
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("Error")
    finally:
        cur.close()
        conn.close()
    
    print("END: "+str(datetime.datetime.now()))
    return tur_tmp
#------------------------------------------------------------------------------------------------------------------
def pgsql_execute(sql,vari,bool_explain,str_db_cluster):
    conn = pgsql_get_connected_object(str_db_cluster)
    cur = conn.cursor()
    tur_tmp = None
    try:
        if bool_explain == True:
            print("Explain Query")
            cur.execute("explain "+sql)
        else:        
            #cur.execute(sql,vari)
            cur.execute(sql)
            conn.commit()
            
        tur_tmp = cur.fetchall()
    except psycopg2.Error as e:
        print("** DB Exception occurs ** "+str(e.pgerror))
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("Error")
    finally:
        cur.close()
        conn.close()

    print("END: "+str(datetime.datetime.now()))
    return tur_tmp
#------------------------------------------------------------------------------------------------------------------
'''
def postgres_copy_from(df_tmp,str_table,str_db_cluster):

    # Pandas dataframe to PostgresSQL using copy command that pretend import CSV to database table
    
    print("Table to import to pgsql: ",str_table)
    print("START: "+str(datetime.datetime.now()))
    print("Dataframe:",df_tmp.shape)

    str_buffer = df_to_str_io_buffer(df_tmp,'\t')
    
    copy_result = 0

    try:
        conn = pgsql_get_connected_object(str_db_cluster)
        #conn.set_session(autocommit=True)
        
        cur = conn.cursor()

        cur.copy_from(str_buffer, str_table, null="")

        conn.commit()
        copy_result = 1
    except psycopg2.Error as e:
        print("** DB Exception occurs ** "+str(e.pgerror))
    except BaseException as e:
        print("Sys error: ",str(e))        
    except:
        print("** Exception occurs **")
    finally:
        conn.close()
    
    print("END: "+str(datetime.datetime.now()))
    print("== Import done ==")
    return copy_result
'''
#------------------------------------------------------------------------------------------------------------------
def postgres_copy_from(df_tmp,str_table,str_db_cluster):

    # Pandas dataframe to PostgresSQL using copy command that pretend import CSV to database table
    
    print("Table to import to pgsql using copy_expert: ",str_table)
    print("START: "+str(datetime.datetime.now()))
    print("Dataframe:",df_tmp.shape)

    str_buffer = df_to_str_io_buffer(df_tmp,',')
    
    copy_result = 0

    try:
        conn = pgsql_get_connected_object(str_db_cluster)
        #conn.set_session(autocommit=True)
        
        cur = conn.cursor()

        cur.copy_expert("COPY "+str_table+" FROM STDOUT DELIMITER AS ',' NULL ''",str_buffer)

        conn.commit()
        copy_result = 1
    except psycopg2.Error as e:
        print("** DB Exception occurs ** "+str(e.pgerror))
    except BaseException as e:
        print("Sys error: ",str(e))        
    except:
        print("** Exception occurs **")
    finally:
        conn.close()
    
    print("END: "+str(datetime.datetime.now()))
    print("== Import done ==")
    return copy_result
#------------------------------------------------------------------------------------------------------------------
def csv_copy_from(csv_path,str_table,str_db_cluster):

    # Pandas dataframe to PostgresSQL using copy command that pretend import CSV to database table
    
    print("Table to import to pgsql using CSV: ",str_table)
    print("START: "+str(datetime.datetime.now()))
    print("Dataframe:",df_tmp.shape)

    try:
        conn = pgsql_get_connected_object(str_db_cluster)
        #conn.set_session(autocommit=True)
        
        cur = conn.cursor()

        cur.copy_expert("COPY "+str_table+" FROM STDOUT DELIMITER AS ','",str_buffer)

        conn.commit()
        copy_result = 1
    except psycopg2.Error as e:
        print("** DB Exception occurs ** "+str(e.pgerror))
    except BaseException as e:
        print("Sys error: ",str(e))        
    except:
        print("** Exception occurs **")
    finally:
        conn.close()
    
    print("END: "+str(datetime.datetime.now()))
    print("== Import done ==")
    return copy_result
#------------------------------------------------------------------------------------------------------------------
def column_str_replace(df_tmp,col_name):
    df_tmp[col_name] = df_tmp[col_name].str.replace('"', '', regex=False)
    df_tmp[col_name] = df_tmp[col_name].str.replace("'", "", regex=False)
    df_tmp[col_name] = df_tmp[col_name].str.replace("\t", "", regex=False)
    df_tmp[col_name] = df_tmp[col_name].str.replace("\\", "", regex=False)
    
    df_tmp[col_name] = df_tmp[col_name].str.replace(",", " ", regex=False)
    df_tmp[col_name] = df_tmp[col_name].str.replace("\n", " ", regex=False)
    df_tmp[col_name] = df_tmp[col_name].str.replace("\r", " ", regex=False)
    return
#------------------------------------------------------------------------------------------------------------------
def int_col_to_na(df_temp,colName,src_val):
    df_temp.loc[df_temp[colName] == src_val,colName] = fm.pd.NA
    return
#------------------------------------------------------------------------------------------------------------------
def df_to_str_io_buffer(df_tmp,sep):
    import io
    str_buffer = io.StringIO()
    df_tmp.to_csv(str_buffer, sep=sep, header=False, index=False)
    str_buffer.seek(0)
    print("DF to str buffer "+str(datetime.datetime.now()))
    return str_buffer
#------------------------------------------------------------------------------------------------------------------
def pgsql_bulk_update(str_tmp_table,df_tmp_table,str_tmp_spec,sql_update,str_db_cluster): 

    str_buffer = df_to_str_io_buffer(df_tmp_table,'\t')

    conn = pgsql_get_connected_object(str_db_cluster)
    cur = conn.cursor()
    print("START: "+str(datetime.datetime.now()))
    print("Dataframe:",df_tmp_table.shape)

    try:
        cur.execute("create temporary table "+str_tmp_table+" ("+str_tmp_spec+")",None)
        conn.commit()
        print("Created tmp table",str_tmp_table)

        cur.copy_from(str_buffer, str_tmp_table, null="")
        conn.commit()
        print("Copied data to tmp table")

        cur.execute(sql_update,None)
        conn.commit()
        print("Executed update")

    except psycopg2.Error as e:
        print("** DB Exception occurs ** "+str(e.pgerror))
    except BaseException as e:
        print("Sys error: ",str(e))        
    except:
        print("Error")
    finally:
        cur.close()
        conn.close()
    
    print("END: "+str(datetime.datetime.now()))
    print("== Update done ==")
    return
#------------------------------------------------------------------------------------------------------------------
def pgsql_bulk_insert(str_tmp_table,str_tar_table,df_tmp_table,arr_tmp_constraint,sql_insert,str_db_cluster): 

    str_buffer = df_to_str_io_buffer(df_tmp_table,'\t')

    conn = pgsql_get_connected_object(str_db_cluster)
    cur = conn.cursor()
    print("START: "+str(datetime.datetime.now()))
    print("Dataframe:",df_tmp_table.shape)

    try:
        cur.execute("create temporary table "+str_tmp_table+" as table "+str_tar_table+" with no data",None)
        conn.commit()
        print("Created empty tmp table",str_tmp_table," with target table structure",str_tar_table)

        for x in arr_tmp_constraint:
            cur.execute(x,None)
            conn.commit()
            print("Created constraint on tmp |",x)
            
        
        cur.copy_from(str_buffer, str_tmp_table, null="")
        conn.commit()
        print("Copied data to tmp table "+str(datetime.datetime.now()))

        cur.execute(sql_insert,None)
        conn.commit()
        print("Executed insert")

    except psycopg2.Error as e:
        print("** DB Exception occurs ** "+str(e.pgerror))
    except BaseException as e:
        print("Sys error: ",str(e))        
    except:
        print("Error")
    finally:
        cur.close()
        conn.close()
    
    print("END: "+str(datetime.datetime.now()))
    print("== Import done ==")
    return
#------------------------------------------------------------------------------------------------------------------
def pgsql_bulk_query(arr_tmp_tables,sql_query,bool_explain,str_db_cluster): 
    
    conn = pgsql_get_connected_object(str_db_cluster)
    cur = conn.cursor()
    print("START: "+str(datetime.datetime.now()))

    df_result = pd.DataFrame()
    try:
        print("Temp tables to upload:",len(arr_tmp_tables))
        
        for x in arr_tmp_tables:
            
            str_buffer = df_to_str_io_buffer(x[1],'\t')
            
            cur.execute("create temporary table "+x[0]+" ("+x[2]+")",None)
            conn.commit()
            print("Created tmp table",x[0],str(datetime.datetime.now()))

            cur.copy_from(str_buffer, x[0], null="")
            conn.commit()
            print("Copied data to tmp table",x[1].shape)

        if bool_explain == True:
            print("Explain Query")
            tur_tmp = None
            cur.execute("explain "+sql_query)
            tur_tmp = cur.fetchall()
            df_result = tur_tmp
        else:
            print("Begin query: "+str(datetime.datetime.now()))
            df_result = pd.read_sql(sql_query, conn)
            print("Result:",df_result.shape)
            
    except psycopg2.Error as e:
        print("** DB Exception occurs ** "+str(e.pgerror))
    except BaseException as e:
        print("Sys error: ",str(e))        
    except:
        print("Error")
    finally:
        cur.close()
        conn.close()
    
    print("END: "+str(datetime.datetime.now()))
    print("== Query done ==")
    return df_result
#------------------------------------------------------------------------------------------------------------------
def remove_issue_comp_duplicate_ocid(tar_table,src_table,str_db_cluster):
    print(tar_table," | ",src_table," | DB:",str_db_cluster)
    
    df_conflict = pgsql_query("select a.* from "+src_table+" a inner join "+tar_table+" b on a.order_id = b.order_id",False,str_db_cluster)
    print(df_conflict.shape)

    if df_conflict.shape[0] > 0:
        print("Remove duplicate in both issue and complete")
        pgsql_execute("drop table if exists ocid_comp_delete",None,False,str_db_cluster)

        pgsql_execute("select a.order_id into ocid_comp_delete from "+tar_table+" a inner join "+src_table+" b on a.order_id = b.order_id",None,False,str_db_cluster)
        pgsql_execute("delete from "+tar_table+" using ocid_comp_delete where "+tar_table+".order_id = ocid_comp_delete.order_id",None,False,str_db_cluster)

        pgsql_execute("drop table if exists ocid_comp_delete",None,False,str_db_cluster)
        print("Remove duplicate done")
    return
#------------------------------------------------------------------------------------------------------------------
