from database import Database
from task import ExcelTask, PythonTask
from task_logger import StandardLogger
from datetime import date

Database.initialize()

# Drops everything in the database
Database.Base.metadata.drop_all(Database.engine)

# Creates the schema from scratch
Database.Base.metadata.create_all(Database.engine)
session = Database.get_session()
"""
task1 = ExcelTask(technical_name='task1',
            start_date=date.today(),
            start_time = '10:00',
            periodicity = 'Daily',
            attrs= {'workbook_path': 'C://pokus.xlsx',
                    'workbook_save_as_path': 'C://pokus2.xlsx',
                    'bex_refresh': True,
                    'send_mail': True,
                    'mail_address': 'ha.ha@ha.com',
                    'mail_subject': 'ahoj',
                    'mail_body': 'bla',
                    'mail_attach_excel': True,
                    'mail_attachment_path': 'asdasd',
                    'call_macro': True,
                    'macro_name': 'asdasd'}
            )

task2 = ExcelTask(technical_name='task2',
            start_date=date.today(),
            start_time = '10:00',
            periodicity = 'Daily',
            attrs= {'workbook_path': 'C://pokus.xlsx',
                    'workbook_save_as_path': 'C://pokus2.xlsx',
                    'bex_refresh': True,
                    'send_mail': True,
                    'mail_address': 'ha.ha@ha.com',
                    'mail_subject': 'ahoj',
                    'mail_body': 'bla',
                    'mail_attach_excel': True,
                    'mail_attachment_path': 'asdasd',
                    'call_macro': True,
                    'macro_name': 'asdasd'}
            )
"""

task1 = PythonTask(technical_name='task1',
            start_date=date.today(),
            start_time = '09:00',
            periodicity = 'Daily',
            attrs= {'package_path': 'C://Users/StigelM/Dropbox/Python/sample_task',
                    'run_file_name': 'run.py',
                    }
            )

session.add(task1)
#session.add(task2)
session.commit()
