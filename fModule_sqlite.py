import pandas as pd
import sqlite3

#========================================================================================

def sqllite3_query(sql,str_path):
    cnxn = sqlite3.connect(str_path)
    
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
        
    return df_MS_Query

#========================================================================================

def sqllite3_execute(sql,str_path):
    cnxn = sqlite3.connect(str_path)
    int_done = 0
    
    try:
        cur = cnxn.cursor()
        cur.execute(sql)
        cnxn.commit()
        int_done = 1
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("General Error")
    finally:      
        cnxn.close()
    
    return int_done