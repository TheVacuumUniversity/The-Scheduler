import socket, select
from database import Database
from task import Task
import time
from config import Config
import win32com.client
from sys import path  #used for this file path in the system
from subprocess import call

class TaskWorker:
    # receives t
    def __init__(self, address, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((address, port))

    def run(self):
        running = True
        self.socket.sendall('ready'.encode('ascii'))
        while running:
            try:
                db_session = self.get_db_session()
                response = self.socket.recv(64).decode('ascii')
                print(f"received task_id {response}")
                self.current_task = db_session.query(Task).filter_by(id=int(response)).one()
                print(f"working on task: {self.current_task.technical_name}")
                self.current_task.in_process = True
                db_session.commit()
                self.do_task()
                db_session.commit()
                db_session.close()
            except (KeyboardInterrupt, ValueError):
                running = False
                self.socket.close()
                print("Client shut down")


    def get_db_session(self):
        Database.initialize()
        return Database.get_session()

    def do_task(self):
        # all methods to perform task will be called from there
        # for now only only waits and marks task as completed
        self.socket.sendall('busy'.encode('ascii'))

        # work
        self.open_excel_file()
        self.bw_connect()
        self.bw_refresh()
        # self.bw_filter_value()
        # self.write_to_excel_file()
        self.run_macro()
        self.close_excel_file()
        self.run_python()
        self.send_mail()

        print("Lets mark it as complete")
        self.current_task.mark_as_completed()
        self.socket.sendall('ready'.encode('ascii'))
        print("Marked as completed")
        print("Waiting for another task")

    #
    def open_excel_file(self):
        # try:
        print(self.current_task.workbook_path)
        if self.current_task.workbook_path != '':
            self.current_task.xl = win32com.client.Dispatch("Excel.Application")
            self.current_task.xl.visible = 1
            self.current_task.workbook = self.current_task.xl.Workbooks.Open(Filename=self.current_task.workbook_path, ReadOnly=0, UpdateLinks=1)
        # except:
        #     # save it to log file
	    #     print('Neotviram')

    # prepared but not user
    def write_to_excel_file(self,Sheet,Cell,Value):
        # self.current_task.workbook.Worksheets(Sheet).Range(Cell).value = Value
        pass

    def close_excel_file(self):
        # try:
        if self.current_task.workbook_save_as_path == '':
            self.current_task.workbook.Save()
        else:
            self.current_task.workbook.saveAs(self.current_task.workbook_save_as_path)

        self.current_task.workbook.Close(False)
        self.current_task.xl.Application.Quit()
    # except:
        #     pass

    def bw_connect(self):
        # try:
            # ALTERNATIVE CONNECTION
            # Run ("BexAnalyzer.xla!Common.MenuConnectionConnect")
            # Application.Wait Now() + TimeValue("00:00:05")
            # Application.SendKeys (Config.bw_password)
            # Application.SendKeys ("{ENTER}")
            # Ignores global settings and connects and activate SAP GUI

        if self.current_task.bex_refresh:
            # self.current_task.xl.Application.DisplayAlerts = False
            self.current_task.xl.Workbooks.Open(path[0] + "\\BexAnalyzer.xla")

            BexAnalyzer = self.current_task.xl.Run("BexAnalyzer.xla!sapbexGetConnection")
            BexAnalyzer.Client = Config.bw_client
            BexAnalyzer.User = Config.bw_user #USERNAME
            BexAnalyzer.Password = Config.bw_password #PASSWORD
            BexAnalyzer.Language = "EN"
            BexAnalyzer.SystemNumber = Config.bw_system_number
            BexAnalyzer.ApplicationServer = Config.bw_ip_address #IP ADRRESS TO YOUR BUSINESS WAREHOUSE SERVER
            BexAnalyzer.SAProuter = ""
            BexAnalyzer.UseSAPLogonIni = False
            BexAnalyzer.Logon(0,True)
            if BexAnalyzer.IsConnected != 1:
                BexAnalyzer.Logon(0,True)
                if BexAnalyzer.IsConnected != 1:
                    BexAnalyzer.Logon(0,True)

            print("Connected")
            self.current_task.xl.Application.Run("BexAnalyzer.xla!sapbexinitConnection")


    def bw_refresh(self):
        self.current_task.xl.Application.Run("BexAnalyzer.xla!SAPBEXrefresh", True)


    def bw_disconnect(self):
        # terminate process SAPlogon.exe
        pass


    # not used yet
    def bw_filter_value(self,provider, fieldTechnicalName, filtredValue):
        pass

    def run_macro(self):
        if self.current_task.macro_name != '' :
            for macro in self.current_task.macro_name.split(";"):
                self.current_task.xl.Application.Run(\
                "'" + self.current_task.workbook_path.split("\\")[-1] + "'!" + macro)


    def send_mail(self):
        if self.current_task.mail_address != '':
            self.current_task.outlook =  win32com.client.Dispatch('outlook.application')
            self.current_task.mail = self.current_task.outlook.CreateItem(0)
            self.current_task.mail.SentOnBehalfOfName = Config.email_send_on_behalf
            self.current_task.mail.To = self.current_task.mail_address
            self.current_task.mail.BCC = Config.email_me
            self.current_task.mail.Subject = self.current_task.mail_subject
            self.current_task.mail.HTMLBody = self.current_task.mail_body  #this field is optional

            # To attach a file to the email:
            if self.current_task.mail_attachment_path:
                for attachment in self.current_task.mail_attachment_path.split(";"):
                    self.current_task.mail.Attachments.Add(attachment)

            #Send mail
            self.current_task.mail.Send()

    def run_python(self):
        if self.current_task.python_script != '' :
            for python_script in self.current_task.python_script.split(";"):
                call("python " + python_script)
