from datetime import datetime, timedelta

from sqlalchemy import Column, String, Integer, Date, DateTime, Boolean
from sqlalchemy.dialects.postgresql import JSON

from database import Database


class Property():
    """Descriptor class used to map task attributes from JSON"""
    def __init__(self, name):
        self.name = name

    def __get__(self, instance, cls):
        return instance.attrs[self.name]

    def __set__(self, instance, value):
        instance.attrs[self.name] = value


class Task(Database.Base):
    # holds all task related attributtes
    # loads from database via SQLA ORM
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True)
    type = Column(String)
    technical_name = Column(String)
    start_date = Column(Date)
    start_time = Column(String)
    periodicity = Column(String)
    next_run = Column(DateTime)
    time_of_completion = Column(DateTime, default=datetime(1900, 1, 1))
    in_process = Column(Boolean, default=False)
    attrs = Column(JSON)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.next_run:
            self.next_run = datetime.combine(self.start_date,
                                             datetime.strptime(self.start_time,
                                                               '%H:%M').time())

    def set_next_run_time(self):
        # Pick appropriate number of days regarding the periodicity
        sample = {'Hourly':[1,'hours'],
                  'Daily':[1,'days'],
                  'Weekly':[7,'days'],
                  'Monthly':[1,'months'],
                  'Yearly':[12,'months']}

        periodicity_adjustment = sample[self.periodicity][0]
        periodicity_move = sample[self.periodicity][1]
        print(periodicity_move)

        # For hourly periodicity
        if periodicity_move == 'hours':
            self.next_run += timedelta(hours=periodicity_adjustment)

        # For daily and weekly periodicity
        if periodicity_move == 'days':
            self.next_run += timedelta(days=periodicity_adjustment)

        # For monthly and yearly tasks
        if  periodicity_move == 'months':
            self.next_run += monthdelta(periodicity_adjustment)

        # move nextRun if periodicityAdjustment > 1 thus weekly, monthly,
        # yearly and nextrun is weekend
        if self.periodicity != 'Daily' and self.periodicity != 'Hourly':
            self.next_run += timedelta(days=1)

    def mark_as_completed(self):
        self.time_of_completion = datetime.now()
        self.set_next_run_time()
        self.in_process = False


class ExcelTask(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = 'excel'

    workbook_path = Property('workbook_path')
    workbook_save_as_path = Property('workbook_save_as_path')
    bex_refresh = Property('bex_refresh')
    send_mail = Property('send_mail')
    mail_address = Property('mail_address')
    mail_subject = Property('mail_subject')
    mail_body = Property('mail_body')
    mail_attach_excel = Property('mail_attach_excel')
    mail_attachment_path = Property('mail_attachment_path')
    call_macro = Property('call_macro')
    macro_name = Property('macro_name')


class PythonTask(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = 'python'

    package_path = Property('package_path')
    run_file_name = Property('run_file_name')
