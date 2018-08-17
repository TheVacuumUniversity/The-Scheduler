from task_logger import StandardLogger
from database import Database

Database.initialize()

logger = StandardLogger('worker')

logger.log_event('normal', 'task_start', 'Task started.')
