from sqlalchemy import Column, String, Integer, DateTime
from database import Database
from datetime import datetime
import socket
import platform


class TaskLog(Database.Base):
    __tablename__ = 'task_log'

    id = Column(Integer, primary_key=True)
    process = Column(String)
    comp_ip = Column(String)
    comp_id = Column(String)
    category = Column(String)
    event = Column(String)
    log_time = Column(DateTime)
    message = Column(String)


class TaskLogger:
    def __init__(self, process, comp_ip=None):
        self.process = process
        self.comp_id = platform.node() or socket.gethostname()
        self.comp_ip = comp_ip or self.get_ip()
        self.db_session = Database.get_session()

    def log_event(self, category, event, message=None):

        task_log = TaskLog(process=self.process,
                           comp_ip=self.comp_ip,
                           comp_id=self.comp_id,
                           category=category,
                           event=event,
                           log_time=datetime.utcnow(),
                           message=message
                           )
        self.db_session.add(task_log)
        self.db_session.commit()

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
