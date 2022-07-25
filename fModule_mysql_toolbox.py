import MySQLdb as myc
import pandas as pd
#------------------------------------------------------------------------------------------------------------------
def mysql_get_connected_object_query():
    return myc.connect(host='10.1.6.182',port = 3306,user='01599158',passwd='Fergend6341',db ='toolbox',use_unicode = True,charset = 'UTF8')
    #return myc.connect(host='10.1.6.182',port = 3306,user='approvallog_enq',passwd='autwCzCuw7Pi7JvA',db ='toolbox',use_unicode = True,charset = 'UTF8')
#------------------------------------------------------------------------------------------------------------------
def mysql_get_connected_object_commit():
    return myc.connect(host='10.1.6.182',port = 3306,user='ApprovLog_IU',passwd='3xICQzlhugV1eelF',db ='toolbox',use_unicode = True,charset = 'UTF8')
#------------------------------------------------------------------------------------------------------------------
def mysql_get_connected_object_UAT():
    return myc.connect(host='10.1.6.182',port = 3306,user='ApprovLog_IU',passwd='3xICQzlhugV1eelF',db ='toolbox_uat',use_unicode = True,charset = 'UTF8')    
#------------------------------------------------------------------------------------------------------------------
def mysql_get_connected_object_ORD12M():
    return myc.connect(host='10.1.6.216',port = 3306,user='ORD_12M',passwd='0y15tFTDHvN7EumR',db ='ORD_12M',use_unicode = True,charset = 'UTF8')    
# ==============================================
def MySQL_Query_Toolbox_UAT(sql):
    # Connect to an existing database
    conn = mysql_get_connected_object_UAT()
    print("Connected to UAT for Query")

    df_tmp = pd.read_sql(sql, conn)
    df_tmp.columns = df_tmp.columns.str.strip().str.lower().str.replace(' ', '_')
    print(df_tmp.shape)
    
    conn.close()
    return df_tmp    
# ==============================================
def MySQL_Commit_UAT(sql):
    conn = mysql_get_connected_object_UAT()
    print("Connected to UAT for Commit")
    
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()
    print("SQL exceuted + committed")
    return
# ==============================================
def MySQL_Query_Toolbox_PROD(sql):
    # Connect to an existing database
    conn = mysql_get_connected_object_query()
    print("Connected to PRODUCTION for Query")

    df_tmp = pd.read_sql(sql, conn)
    df_tmp.columns = df_tmp.columns.str.strip().str.lower().str.replace(' ', '_')
    print(df_tmp.shape)
    
    conn.close()
    return df_tmp    
# ==============================================
def MySQL_Commit_PROD(sql):
    conn = mysql_get_connected_object_commit()
    print("Connected to PRODUCTION for Commit")
    
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()
    print("SQL exceuted + committed")
    return
# ==============================================
'''
def MySQL_LoadDataCSV_UAT(sql,csvPath):
    conn = mysql_get_connected_object_UAT()
    print("Connected to UAT for Load Data in File")
    
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()
    print("SQL exceuted + committed")
    return
'''
# ==============================================
'''
def MySQL_Query_Toolbox_Cursor(sql):
    conn= myc.connect(
            host='10.1.6.182',
            port = 3306,
            user='approvallog_enq',
            passwd='autwCzCuw7Pi7JvA',
            db ='toolbox',
            )
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    conn.close()
    print("Result len: "+str(len(result)))
    return result
'''
# ==============================================
def staffdb_staffid_check(arr_staffid):
    sql=r"select Staff_ID,Staff_Status,Channel_Group,Team,Sub_Team,Location,internal_position,hired_type from staffdb.stafflist_basic where Staff_ID in ("+arr_staffid+");"

    df_staffdb = MySQL_Query_Toolbox_PROD(sql)
    print("StaffDB result size: ",df_staffdb.shape)
    return df_staffdb
# ==============================================
def staffdb_stafflist(str_staff_status):
    if (str_staff_status != None):
        print("Check with staff status",str_staff_status)
        sql=r"select Staff_ID,Staff_Status,Channel_Group,Team,Sub_Team,Location,internal_position,hired_type from staffdb.stafflist_basic where Staff_Status in ("+str_staff_status+");"
    else:
        print("Check with all staff status")
        sql=r"select Staff_ID,Staff_Status,Channel_Group,Team,Sub_Team,Location,internal_position,hired_type from staffdb.stafflist_basic;"

    df_staffdb = MySQL_Query_Toolbox_PROD(sql)
    print("StaffDB result size: ",df_staffdb.shape)
    return df_staffdb
# ==============================================