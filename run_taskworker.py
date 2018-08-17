from taskworker import TaskWorker

worker = TaskWorker('127.0.0.1', 8000)
worker.run()
