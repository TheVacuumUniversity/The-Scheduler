from taskmaster import TaskMaster

server = TaskMaster('127.0.0.1', 8000, 2)
server.run()
