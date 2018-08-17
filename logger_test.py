from task_logger import TaskLogger
from database import Database

Database.initialize()

logger = TaskLogger('worker')

logger.log_event('normal', 'task_start', 'Task started.')
