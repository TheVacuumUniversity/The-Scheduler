import socket
import select
import platform
import time
import subprocess
from sys import path  # used for this file path in the system

from database import Database
from task import ExcelTask, PythonTask
from task_logger import StandardLogger


class TaskWorker:
    """
    Receives task id from TaskMaster, loads task data from database, executes
    the task.
    :param address: IP address (string) on which to bind worker socket
    :param port: port(integer) on which to bind worker socker
    """
    def __init__(self, address, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((address, port))
        self.comp_name = platform.node() or socket.gethostname()
        self.comp_ip = self.get_ip()

    def run(self):
        running = True

        self.init_conn()

        while running:
            try:
                db_session = self.get_db_session()
                response = self.receive()
                print(f"received task_id {response}")
                self.current_task = db_session.query(PythonTask).filter_by(id=int(response)).one()
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

    def send(self, msg):
        self.socket.sendall(msg.encode('ascii'))

    def receive(self):
        return self.socket.recv(1024).decode('ascii')

    def init_conn(self):
        # send master a name
        self.send(self.comp_name)
        # wait for confirmation
        msg = self.receive()
        if msg == 'ok':
            print('name received by master')
        # send master an ip
        self.send(self.comp_ip)
        # wait for confirmation
        msg = self.receive()
        if msg == 'ok':
            print('ip received by master')
        # send ready message
        self.send('ready')

    def get_db_session(self):
        Database.initialize()
        return Database.get_session()

    def get_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # doesn't even have to be reachable
            s.connect(('10.255.255.255', 1))
            ip_add = s.getsockname()[0]
        except:
            ip_add = '127.0.0.1'
        finally:
            s.close()
        return ip_add

    def do_task(self):
        self.socket.sendall('busy'.encode('ascii'))

        # work
        python_env = self.current_task.package_path + '/venv/Scripts/python.exe'
        file_path = self.current_task.package_path + '/' + self.current_task.run_file_name
        result = subprocess.run([python_env, file_path],
                                stderr=subprocess.PIPE
                               )

        if result.stderr:
            # will need to log into database or pass to master?
            print(result.stderr)
        else:
            print("Lets mark it as complete")
            self.current_task.mark_as_completed()
            self.send('ready')
            print("Marked as completed")
            print("Waiting for another task")


class ExcelTaskWorker:
    # receives t
    def __init__(self, address, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((address, port))
        self.comp_name = platform.node() or socket.gethostname()
        self.comp_ip = self.get_ip()

    def run(self):
        running = True
        # send master a name
        self.socket.sendall(self.comp_name.encode('ascii'))
        # wait for confirmation
        msg = self.socket.recv(1024).decode('ascii')
        if msg == 'ok':
            print('name received by master')
        # send master an ip
        self.socket.sendall(self.comp_ip.encode('ascii'))
        # wait for confirmation
        msg = self.socket.recv(1024).decode('ascii')
        if msg == 'ok':
            print('ip received by master')
        # send ready message
        self.socket.sendall('ready'.encode('ascii'))

        while running:
            try:
                db_session = self.get_db_session()
                response = self.socket.recv(1024).decode('ascii')
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

    def get_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # doesn't even have to be reachable
            s.connect(('10.255.255.255', 1))
            ip_add = s.getsockname()[0]
        except:
            ip_add = '127.0.0.1'
        finally:
            s.close()
        return ip_add

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
