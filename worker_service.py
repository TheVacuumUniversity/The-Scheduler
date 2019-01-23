from base_winservice import BaseWinservice
from taskworker import TaskWorker


class WorkerService(BaseWinservice):
    _svc_name_ = 'worker_service'
    _svc_display_name_ = "Scheduler Task Worker"
    _svc_description_ = "Service to execute scheduled tasks given by master"

    def main(self):
        worker = TaskWorker('127.0.0.1', 8000)
        worker.run()

if __name__ == '__main__':
    WorkerService.parse_command_line()
