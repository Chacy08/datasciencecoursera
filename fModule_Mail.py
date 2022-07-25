import win32com.client as win32
win32c = win32.constants
import sys
# ========================================================================================================================================
def send_mail(subject,to_list,cc_list,html_content,attach_link,is_debug):   
    #print(f"Sending mail: ",subject,to_list,cc_list,html_content,attach_link,is_debug)
    print("Using M365, this function not working anymore")
    
    success = 0
    '''
    try:
        o = win32.gencache.EnsureDispatch("Outlook.Application")

        Msg = o.CreateItem(0)
        Msg.To = to_list
        Msg.CC = cc_list

        Msg.Recipients.ResolveAll()
        
        Msg.Subject = subject

        Msg.HTMLBody = html_content

        if (len(attach_link) > 0):
            Msg.Attachments.Add(attach_link)

        if (is_debug == True):
            Msg.Display()
        else:
            Msg.Send()
        
        print("e-mail sent")
        success = 'Sent'
    except:
        #print("Unexpected error:", sys.exc_info())
        success = sys.exc_info()
    '''
    return success
# ========================================================================================================================================
def win32com_clear_cache():
    # Delete previous win32com gen cache
    import shutil

    directory = r"C:\Users\H1599158\AppData\Local\Temp\gen_py\3.9"
    shutil.rmtree(directory)
    return
# ========================================================================================================================================
def run_access_vba_macro(access_path,macro_name,sub_name):
    
    try:
        o_Access = win32.gencache.EnsureDispatch("Access.Application")
        o_Access.Visible = 0 # Not show Access window    
        
        o_Access.OpenCurrentDatabase(access_path,True)
        print("Opened access: "+access_path)

        if sub_name != None:
            o_Access.Run(sub_name)
            print("Executed Sub: "+sub_name)
        elif macro_name != None:
            o_Access.DoCmd.RunMacro(macro_name)
            print("Executed Macro: "+macro_name)
        else:
            print("No action found")

        o_Access.Quit(win32c.acQuitSaveAll)
        print("Saved and closed access")
    except:
        print("Error:",sys.exc_info())
        
    return
# ========================================================================================================================================
def encrypt_attachment(str_file_to_zip,str_dest_path,str_password):
    import subprocess

    str_success = "Failed"
    str_software_path = r"D:\SoftwareTEMP\7z1900-extra\7za.exe"
    print("File to encrypt",str_file_to_zip)
    print("File destination",str_dest_path)
    try:
        str_success = subprocess.run(str_software_path+' a "'+str_dest_path+'" "'+str_file_to_zip+'" -p'+str_password)
    except BaseException as e:
        print("Sys error: ",str(e))
    except:
        print("Error:",sys.exc_info())
    return str_success

# ========================================================================================================================================

