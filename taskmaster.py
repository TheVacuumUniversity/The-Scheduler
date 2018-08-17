import socket, select
from database import Database
from queue import Queue
from task_logger import StandardLogger
from datetime import datetime
import time
import threading
import traceback
from task import Task
from sqlalchemy import and_

class TaskMaster:
    # listens to incoming TaskWorker connections and accepts it
    # has separate thread checking for new due tasks
    # load due tasks and sends task id to available worker

    def __init__(self, address, port, queue_refresh_interval):
        Database.initialize()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.setblocking(0)
        self.socket.bind((address, port))
        self.socket.listen(5)
        self.address = f"{address}:{port}"
        self.in_sockets = [self.socket]
        self.out_sockets = []
        self.workers_info = {}
        self.task_queue = []
        self.queue_refresh_interval = queue_refresh_interval
        self.task_queue_refreshing = True
        self.task_queue_lock = threading.Lock()
        self.logger = StandardLogger('master')

    def __repr__(self):
        return "< TaskMaster server running on {self.address} >"

    def task_queue_refresher(self):
        while self.task_queue_refreshing:
            self.update_task_queue()
            time.sleep(self.queue_refresh_interval)
        self.db_session.commit()
        print("refresher: good bye")

    def run(self):
        self.logger.log_event('server_start', 'Server started')
        self.queue_refresher_thread = threading.Thread(name='queue_refresher_thread', target=self.task_queue_refresher)
        try:
            self.db_session = self.get_db_session()
            self.queue_refresher_thread.start()
            time.sleep(5)
            while True:

                readable, writable, exceptional = select.select(self.in_sockets,
                                                self.out_sockets, self.in_sockets, 1)

                if readable:
                    #print('checking readable')
                    for s in readable:
                        if s is self.socket:
                            self.accept_client(s)
                        else:
                            msg = self.receive_msg(s)
                            # master is expecting comp_name as first response from client
                            if not self.workers_info[s]['comp_name']:
                                 self.workers_info[s]['comp_name'] = msg
                                 self.workers_info[s]['msg_queue'].put('ok')
                                 self.out_sockets.append(s)
                            # then comp_ip
                            elif not self.workers_info[s]['comp_ip']:
                                self.workers_info[s]['comp_ip'] = msg
                                self.workers_info[s]['msg_queue'].put('ok')
                                self.out_sockets.append(s)
                                self.logger.log_event('worker_connected', f"Worker {self.workers_info[s]['comp_ip']} connected to the server")
                            # then status of worker
                            elif msg in ['ready', 'busy']:
                                self.workers_info[s]['status'] = msg
                                if self.workers_info[s]['status'] == 'ready':
                                    self.out_sockets.append(s)
                                elif self.workers_info[s]['status'] == 'busy':
                                    self.out_sockets.remove(s)
                            else:
                              self.discard_socket(s)

                if writable and self.task_queue:
                    #print('checking writeable')
                    for s in writable:
                        if not self.workers_info[s]['msg_queue'].empty():
                            msg = self.workers_info[s]['msg_queue'].get_nowait()
                            s.send(msg.encode('ascii'))
                            self.out_sockets.remove(s)
                        elif self.task_queue and self.workers_info[s]['status'] == 'ready': # send msg only if there is task in queue
                            self.assing_next_task(s)
                            self.workers_info[s]['status'] = 'busy' # master has to mark worker as busy immediately as if worker is to send busy status it might not send it prior to next writable check

                    for s in exceptional:
                        self.discard_socket(s)

        except KeyboardInterrupt:
            self.logger.log_event('keyboard_interrupt', 'User requested shutdown by keyboard interrupt')
            self.shut_down()
        except Exception as e:
            # catching all other exceptions and logging them to the database
            trace_msg = traceback.format_exc()
            self.logger.log_event('runtime_error', trace_msg)

    def get_db_session(self):
        Database.initialize()
        return Database.get_session()

    def update_task_queue(self):
        due_tasks = self.db_session.query(Task).filter(and_(Task.next_run < datetime.now(), Task.in_process != True)).order_by(Task.next_run)
        self.task_queue_lock.acquire()
        tasks_cnt = len(self.task_queue)
        for task in due_tasks:
            if task not in self.task_queue:
                self.task_queue.append(task)
        if tasks_cnt != len(self.task_queue):
            print(f"refresher: new tasks in queue: {len(self.task_queue)}")
        self.task_queue_lock.release()
        #db_session.close()

    def discard_socket(self, socket):
        self.in_sockets.remove(socket)
        if socket in self.out_sockets:
            self.out_sockets.remove(socket)
        socket.close()
        del self.workers_info[socket]

    def accept_client(self, socket):
        conn, client_ip = socket.accept()
        print(client_ip)
        conn.setblocking(0)
        self.in_sockets.append(conn)
        self.workers_info[conn] = {}
        self.workers_info[conn]['msg_queue'] = Queue()
        self.workers_info[conn]['comp_name'] = None
        self.workers_info[conn]['comp_ip'] = None

    def send_msg(self, socket, task_id):
        print(f"sending task id: {task_id}")
        socket.send(str(task_id).encode('ascii'))
        self.out_sockets.remove(socket)

    def receive_msg(self, socket):
        msg = socket.recv(1024).decode('ascii')
        print(f"received message: {msg}")
        return msg

    def assing_next_task(self, socket):
        self.task_queue_lock.acquire()
        if self.task_queue:
            task = self.task_queue[0]
            self.task_queue = self.task_queue[1:]
            self.send_msg(socket, task.id)
            self.logger.log_event('task_assigned',
                             f"Master assigned task id {task.id} to worker {self.workers_info[socket]['comp_name']}, {self.workers_info[socket]['comp_ip']}")
            self.out_sockets.append(socket)
        self.task_queue_lock.release()

    def shut_down(self):
        print("Shutting down sockets...")
        for s in set(self.in_sockets + self.out_sockets):
            s.close()
        print("Shutting down refresher...")
        self.task_queue_refreshing = False
        self.queue_refresher_thread.join()

        print("Shutting down server...")
        self.db_session.close()
        self.logger.log_event('shut_down', 'Server shut down by user command')
