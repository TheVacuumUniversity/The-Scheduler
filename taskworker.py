import socket, select
import platform
from database import Database
from task import Task
from task_logger import StandardLogger
import time

class TaskWorker:
    # receives t
    def __init__(self, address, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((address, port))
        self.comp_name = platform.node() or socket.gethostname()
        self.comp_ip = self.get_ip()

    def run(self):
        running = True
        # send master a name
        self.socket.sendall(self.comp_name.encode('ascii'))
        # wait for confirmation
        msg = self.socket.recv(1024).decode('ascii')
        if msg == 'ok':
            print('name received by master')
        # send master an ip
        self.socket.sendall(self.comp_ip.encode('ascii'))
        # wait for confirmation
        msg = self.socket.recv(1024).decode('ascii')
        if msg == 'ok':
            print('ip received by master')
        # send ready message
        self.socket.sendall('ready'.encode('ascii'))

        while running:
            try:
                db_session = self.get_db_session()
                response = self.socket.recv(1024).decode('ascii')
                print(f"received task_id {response}")
                self.current_task = db_session.query(Task).filter_by(id=int(response)).one()
                print(f"working on task: {self.current_task.technical_name}")
                self.current_task.in_process = True
                db_session.commit()
                self.do_task()
                db_session.commit()
                db_session.close()
            except (KeyboardInterrupt, ValueError):
                running = False
                self.socket.close()
                print("Client shut down")

    def get_db_session(self):
        Database.initialize()
        return Database.get_session()

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

    def do_task(self):
        # all methods to perform task will be called from there
        # for now only only waits and marks task as completed
        self.socket.sendall('busy'.encode('ascii'))
        time.sleep(5)
        self.current_task.mark_as_completed()
        self.socket.sendall('ready'.encode('ascii'))
