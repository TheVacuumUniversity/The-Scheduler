from database import Database
from task import Task
from task_logger import TaskLogger
from datetime import date

Database.initialize()

# Drops everything in the database
Database.Base.metadata.drop_all(Database.engine)

# Creates the schema from scratch
Database.Base.metadata.create_all(Database.engine)
session = Database.get_session()

task1 = Task(technical_name='task1',
            start_date=date.today(),
            start_time = '16:00',
            periodicity = 'Daily',
            workbook_path = '\\\\seczefnpbrn003\\BRNO FSC\\BI\\Databases\\PostgreLoad\\Other\\Material.xlsm',
            workbook_save_as_path = '',
            bex_refresh = True,
            mail_address = 'pavel.fryblik@edwardsvacuum.com',
            mail_subject = 'ahoj',
            mail_body = 'bla',
            mail_attach_excel = True,
            mail_attachment_path = '',
            macro_name = 'thisworkbook.refreshme',
            python_script = ''
            )

task2 = Task(technical_name='task2',
            start_date=date.today(),
            start_time = '10:00',
            periodicity = 'Daily',
            workbook_path = '\\\\seczefnpbrn003\\BRNO FSC\\BI\\Databases\\PostgreLoad\\Other\\Territory.xlsm',
            workbook_save_as_path = '',
            bex_refresh = False,
            mail_address = 'pavel.fryblik@edwardsvacuum.com',
            mail_subject = 'ahoj',
            mail_body = 'bla',
            mail_attach_excel = True,
            mail_attachment_path = '',
            macro_name = '',
            python_script = 'K:\BI\PythonScheduler\PythonScheduler\pokus.py'
            )

session.add(task1)
session.add(task2)
session.commit()
