import socket
import select
import time
import threading
import traceback
from queue import Queue
from datetime import datetime

from sqlalchemy import and_

from task import Task
from task_logger import StandardLogger
from database import Database


class TaskMaster:
    """
    Listens to incoming TaskWorker connections and accepts it, has separate
    thread checking for new due tasks, loads due tasks and sends task id to
    available worker.

    refer to
    'https://github.com/stigrlm/server_sample/blob/master/server_multi.py'
    to get idea how basic functionality of the server is implemented
    """

    def __init__(self, address, port, queue_refresh_interval):
        """
        Initial function
        :param address: IP address (string) on which to bind server socket
        :param port: port(integer) on which to bind server socker
        :param queue_refresh_interval: time in seconds (integer) how often
            task queue should be refreshed
        """
        self.db_session = self.get_db_session()
        # create TCP socket for the server, AF_INET-socket will be IPv4
        # SOCK_STREAM - TCP socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # make this a non-blocking socket
        self.socket.setblocking(0)
        # bind the socket the IP and port provided
        self.socket.bind((address, port))
        # make it listen to incoming connections
        self.socket.listen(5)
        # string representation of IP and port
        self.address = f"{address}:{port}"
        # reference for sockets where incoming messages should be checked
        self.in_sockets = [self.socket]
        # reference for sockets to whom message should be sent
        self.out_sockets = []
        # info of all connected worker sockets, key is worker socket, value is
        # message queue for given worker socket
        self.workers_info = {}
        # holds due tasks not assigned yet to worker
        self.task_queue = []
        self.queue_refresh_interval = queue_refresh_interval
        # flag wheter queue refreshing thread is working
        self.task_queue_refreshing = True
        self.task_queue_lock = threading.Lock()
        self.logger = StandardLogger('master')


    def __repr__(self):
        return "< TaskMaster server running on {self.address} >"

    def task_queue_refresher(self):
        """Periodically checks if new tasks are due in database"""
        while self.task_queue_refreshing:
            self.update_task_queue()
            time.sleep(self.queue_refresh_interval)
        self.db_session.commit()
        print("refresher: good bye")

    def run(self):
        """Starts the server"""
        try:
            self.logger.log_event('server_start', 'Server started')
            # define queue refresher to be run in separate thread
            self.queue_refresher_thread = threading.Thread(name='queue_refresher_thread',
                                                           target=self.task_queue_refresher)

            #self.db_session = self.get_db_session()
            self.queue_refresher_thread.start()

            self.run_server()

        except KeyboardInterrupt:
            self.logger.log_event('keyboard_interrupt',
                                  "User requested shutdown by keyboard interrupt")
            self.shut_down()

    def run_server(self):
        """Handles the the server logic"""
        try:
            while True:
                print("Checking sockets...")
                readable, writable, exceptional = select.select(self.in_sockets,
                                                self.out_sockets, self.in_sockets, 1)

                if readable:
                    print('checking readable')
                    for s in readable:
                        if s is self.socket:
                            self.last_socket = s
                            self.accept_client(s)
                        else:
                            msg = self.receive_msg(s)
                            # master is expecting comp_name as first response from client
                            if not self.workers_info[s]['comp_name']:
                                self.set_workers_name(s, msg)
                            # then comp_ip
                            elif not self.workers_info[s]['comp_ip']:
                                self.set_workers_ip(s, msg)
                            # then status of worker
                            elif msg in ('ready', 'busy'):
                                self.handle_worker(s, msg)
                            else:
                                self.discard_socket(s)

                if writable and self.task_queue:
                    print("checking writeable")
                    for s in writable:
                        self.last_socket = s
                        # send message to worker - other than new task
                        # e.g. 'ok' confirmation response
                        if not self.workers_info[s]['msg_queue'].empty():
                            msg = self.workers_info[s]['msg_queue'].get_nowait()
                            s.send(msg.encode('ascii'))
                            self.out_sockets.remove(s)
                        # assign new task
                        elif self.task_queue and self.workers_info[s]['status'] == 'ready':
                            self.assing_next_task(s)
                            # master has to mark worker as busy immediately
                            # as if worker is to send busy status it might
                            # not send it prior to next writable check
                            self.workers_info[s]['status'] = 'busy'

                if exceptional:
                    for s in exceptional:
                        self.last_socket = s
                        self.discard_socket(s)

        # in case worker is shutdown unexpectedly
        except (ConnectionAbortedError, ConnectionResetError) as e:
            print('Worker closed connection')
            self.logger.log_event('worker_connection_abort', str(e))
            self.discard_socket(self.last_socket)
            self.run_server()
        # catching all other exceptions and logging them to the database
        except Exception as e:
            trace_msg = traceback.format_exc()
            self.logger.log_event('runtime_error', trace_msg)
            self.discard_socket(self.last_socket)
            self.run_server()

    def get_db_session(self):
        """Initialize database and returns session"""
        Database.initialize()
        return Database.get_session()

    def update_task_queue(self):
        """Performs check for due tasks in database and updates task queue"""
        due_tasks = self.db_session.query(Task).filter(and_(Task.next_run < datetime.now(),
                                                            Task.in_process != True)
                                                      ).order_by(Task.next_run)
        self.task_queue_lock.acquire()
        tasks_cnt = len(self.task_queue)
        for task in due_tasks:
            if task not in self.task_queue:
                self.task_queue.append(task)
        if tasks_cnt != len(self.task_queue):
            print(f"refresher: new tasks in queue: {len(self.task_queue)}")
        self.task_queue_lock.release()

    def set_workers_name(self, s, name):
        """
        Updates workers name in workers_info and puts 'ok' message to output queue
        :param s: socket object
        :param name: workers name(string)
        """
        self.workers_info[s]['comp_name'] = name
        self.workers_info[s]['msg_queue'].put('ok')
        self.out_sockets.append(s)

    def set_workers_ip(self, s, ip):
        """
        Updates workers ip in workers_info and puts 'ok' message to output queue
        :param s: socket object
        :param ip: workers ip(string)
        """
        self.workers_info[s]['comp_ip'] = ip
        self.workers_info[s]['msg_queue'].put('ok')
        self.out_sockets.append(s)
        self.logger.log_event('worker_connected',
                              f"Worker {self.workers_info[s]['comp_ip']} connected to the server")

    def handle_worker(self, s, status):
        """
        Updates workers status and based on it decides whether to put its socket
        to outbound - which will expedite task to it in next cycle
        :param s: socket object
        :param status: status from worker(string)
        """
        self.workers_info[s]['status'] = status
        if self.workers_info[s]['status'] == 'ready':
            self.out_sockets.append(s)
        elif self.workers_info[s]['status'] == 'busy':
            self.out_sockets.remove(s)

    def discard_socket(self, socket):
        """
        Removes socket completely from all registers
        :param socket: socket object
        """
        print('removing socket {}'.format(socket))
        self.in_sockets.remove(socket)
        if socket in self.out_sockets:
            self.out_sockets.remove(socket)
        socket.close()
        del self.workers_info[socket]

    def accept_client(self, socket):
        """
        Accepts worker connection and keeps track of worker details
        :param socket: socket object
        """
        conn, client_ip = socket.accept()
        print(client_ip)
        conn.setblocking(0)
        self.in_sockets.append(conn)
        self.workers_info[conn] = {}
        self.workers_info[conn]['msg_queue'] = Queue()
        self.workers_info[conn]['comp_name'] = None
        self.workers_info[conn]['comp_ip'] = None

    def send_msg(self, socket, task_id):
        """
        Sends task id to worker
        :param socket: socket object
        :param task_id: int id of task
        """
        print(f"sending task id: {task_id}")
        socket.send(str(task_id).encode('ascii'))
        self.out_sockets.remove(socket)

    def receive_msg(self, socket):
        """
        Receives message from socket
        :param socket: socket object
        """
        msg = socket.recv(1024).decode('ascii')
        print(f"received message: {msg}")
        return msg

    def assing_next_task(self, socket):
        """
        Assigns next task from queue to worker
        :param socket: socket object
        """
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
        """Shuts down server"""
        print("Shutting down sockets...")
        for s in set(self.in_sockets + self.out_sockets):
            s.close()
        print("Shutting down refresher...")
        self.task_queue_refreshing = False
        self.queue_refresher_thread.join()

        print("Shutting down server...")
        self.db_session.close()
        self.logger.log_event('shut_down', 'Server shut down by user command')
