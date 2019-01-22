from base_winservice import BaseWinservice
from taskmaster import TaskMaster


class MasterService(BaseWinservice):
    _svc_name_ = 'master_service'
    _svc_display_name_ = "Scheduler Task Master"
    _svc_description_ = "Service to schedule regular tasks"

    def main(self):
        server = TaskMaster('127.0.0.1', 8000, 2)
        server.run()

if __name__ == '__main__':
    MasterService.parse_command_line()
