# shutil for file system operation
import shutil
import glob
# ========================================================================================================================================
def copy_replace_file(src_path,dest_path):
    try: 
        print("Copying:",src_path)
        print("Destination:",dest_path)
        shutil.copyfile(src_path, dest_path) 
        print("File copied successfully.") 
        return 1
        
    # If source and destination are same 
    except shutil.SameFileError: 
        print("Source and destination represents the same file.") 
        return 0
    # If destination is a directory. 
    except IsADirectoryError: 
        print("Destination is a directory.") 
        return 0
    # If there is any permission issue 
    except PermissionError: 
        print("Permission denied.") 
        return 0
    # For other errors
    except: 
        print("Error occurred while copying file.")    
        return 0
# ========================================================================================================================================
def copy_replace_curr_custsubr(str_mth):
    result = copy_replace_file(r"\\10.1.106.217\EDW_CUST_SUBR_"+str_mth+".accdb",r"D:\EDW_CUST_SUBR_"+str_mth+".accdb")
    return result
# ========================================================================================================================================
def checkFileExist(fullpath):
    filePath = glob.glob(fullpath)
    print("checkFileExist: ",filePath[:2])
    return filePath