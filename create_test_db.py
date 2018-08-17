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
            start_time = '10:00',
            periodicity = 'Daily',
            workbook_path = 'C://pokus.xlsx',
            workbook_save_as_path = 'C://pokus2.xlsx',
            bex_refresh = True,
            send_mail = True,
            mail_address = 'ha.ha@ha.com',
            mail_subject = 'ahoj',
            mail_body = 'bla',
            mail_attach_excel = True,
            mail_attachment_path = 'asdasd',
            call_macro = True,
            macro_name = 'asdasd'
            )

task2 = Task(technical_name='task2',
            start_date=date.today(),
            start_time = '10:01',
            periodicity = 'Daily',
            workbook_path = 'C://pokus.xlsx',
            workbook_save_as_path = 'C://pokus2.xlsx',
            bex_refresh = True,
            send_mail = True,
            mail_address = 'ha.ha@ha.com',
            mail_subject = 'ahoj',
            mail_body = 'bla',
            mail_attach_excel = True,
            mail_attachment_path = 'asdasd',
            call_macro = True,
            macro_name = 'asdasd'
            )

session.add(task1)
session.add(task2)
session.commit()
