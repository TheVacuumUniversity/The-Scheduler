from database import Database
from sqlalchemy import Column, String, Integer, Date, DateTime, Boolean
from datetime import datetime, timedelta

class Task(Database.Base):
    # holds all task related attributtes
    # loads from database via SQLA ORM
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True)
    technical_name = Column(String)
    start_date = Column(Date)
    start_time = Column(String)
    periodicity = Column(String)
    workbook_path = Column(String)
    workbook_save_as_path = Column(String)
    bex_refresh = Column(Boolean)
    mail_address = Column(String)
    mail_subject = Column(String)
    mail_body = Column(String)
    mail_attach_excel = Column(Boolean)
    mail_attachment_path = Column(String)
    macro_name = Column(String)
    python_script = Column(String)
    next_run = Column(DateTime)
    time_of_completion = Column(DateTime, default=datetime(1900, 1, 1))
    in_process = Column(Boolean, default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.next_run:
            self.next_run = datetime.combine(self.start_date,datetime.strptime(self.start_time, '%H:%M').time())


    def set_next_run_time(self):
        # Pick appropriate number of days regarding the periodicity
        sample = {'Hourly':[1,'hours'],'Daily':[1,'days'],'Weekly':[7,'days'],'Monthly':[1,'months'],'Yearly':[12,'months']}
        periodicity_adjustment = sample[self.periodicity][0]
        periodicity_move = sample[self.periodicity][1]

        while self.next_run <= self.time_of_completion:
            #For hourly periodicity
            if periodicity_move == 'hours':
                self.next_run += timedelta(hours=periodicity_adjustment)

            #For daily and weekly periodicity
            if periodicity_move == 'days':
                self.next_run += timedelta(days=periodicity_adjustment)

            #For monthly and yearly tasks
            if  periodicity_move == 'months':
                self.next_run += monthdelta(periodicity_adjustment)

            # move nextRun if periodicityAdjustment > 1 thus weekly, monthly, yearly and nextrun is weekend
            if self.periodicity != 'Daily' and self.periodicity != 'Hourly':
                while self.nextRun.weekday() > 4:
                    self.next_run += timedelta(days=1)

    def mark_as_completed(self):
        self.time_of_completion = datetime.now()
        self.set_next_run_time()
        self.in_process = False
