from collections import OrderedDict
import time
import logging
import ConfigParser
import zmq
import os

class _QueueConf(object):
    def __init__(self):
        self.__conf = ConfigParser.ConfigParser()
        hasFile = False
        if os.path.isfile("ZeroMQQueue.conf"):
            self.__conf.read("ZeroMQQueue.conf")
            logging.info("Using configuration file at ZeroMQQueue.conf")
            hasFile = True
        else:
            logging.warn("Could not find ZeroMQQueue.conf, Use default setting now")
        self.frontEndPort = self.__conf.getint("ZeroMQQueueServer", "FrontEndPort") if hasFile else 5555
        self.backEndPort = self.__conf.get("ZeroMQQueueServer", "BackendPort") if hasFile else 5556
        self.workerLiveness = self.__conf.getfloat("Worker", "HeartBeatLiveness") if hasFile else 3.0
        self.heartbeatInterval = self.__conf.getfloat("Worker", "HeartBeatInterval") if hasFile else 1.0
    
    def getFrontEndPort(self):
        return self.frontEndPort

    def getBackEndPort(self):
        return self.backEndPort

    def getWorkerHeartBeatLiveness(self):
        return self.workerLiveness

    def getHeartBeatInterval(self):
        return self.heartbeatInterval



class _Worker(object):
    def __init__(self, address, heartbeatLiveness, heartbeatInterval):
        self.address = address
        self.expiry = time.time() + heartbeatLiveness * heartbeatInterval
        

class WorkerQueue(object):
    def __init__(self):
        self.queue = OrderedDict()
        self.context = None
        self.frontend = None
        self.backend = None
        self.poll_both = None
        self.poll_workers = None
        self.heartbeat_at = 0.0
        self.__conf = _QueueConf()
        self.HEARTBEAT_LIVENESS = self.__conf.getWorkerHeartBeatLiveness()
        self.HEARTBEAT_INTERVAL = self.__conf.getHeartBeatInterval()

        #  Paranoid Pirate Protocol constants
        self.PPP_READY = "\x01"      # Signals worker is ready
        self.PPP_HEARTBEAT = "\x02"  # Signals worker heartbeat

    def connect(self):
        self.context = zmq.Context(1)
        self.frontend = self.context.socket(zmq.ROUTER)
        self.backend = self.context.socket(zmq.ROUTER)
        self.frontend.bind("tcp://*:{0}".format(self.__conf.getFrontEndPort())) # For clients
        self.backend.bind("tcp://*:{0}".format(self.__conf.getBackEndPort()))  # For workers

        self.poll_workers = zmq.Poller()
        self.poll_workers.register(self.backend, zmq.POLLIN)

        self.poll_both = zmq.Poller()
        self.poll_both.register(self.frontend, zmq.POLLIN)
        self.poll_both.register(self.backend, zmq.POLLIN)
        self.heartbeat_at = time.time() + self.HEARTBEAT_INTERVAL


    def ready(self, worker):
        self.queue.pop(worker.address, None)
        self.queue[worker.address] = worker

    def run(self):
        heartbeat_at = time.time() + self.HEARTBEAT_INTERVAL

        while True:
            if len(self.queue) > 0:
                poller = self.poll_both
            else:
                poller = self.poll_workers
            socks = dict(poller.poll(self.HEARTBEAT_INTERVAL * 1000))

            # Handle worker activity on backend
            if socks.get(self.backend) == zmq.POLLIN:
                # Use worker address for LRU routing
                frames = self.backend.recv_multipart()
                if not frames:
                    break

                address = frames[0]
                self.ready(_Worker(address, self.HEARTBEAT_LIVENESS, self.HEARTBEAT_INTERVAL))

                # Validate control message, or return reply to client
                msg = frames[1:]
                if len(msg) == 1:
                    if msg[0] not in (self.PPP_READY, self.PPP_HEARTBEAT):
                        logging.error("Invalid message from worker: %s" % msg)
                else:
                    self.frontend.send_multipart(msg)

                # Send heartbeats to idle workers if it's time
                if time.time() >= heartbeat_at:
                    for worker in self.queue:
                        msg = [worker, self.PPP_HEARTBEAT]
                        self.backend.send_multipart(msg)
                    heartbeat_at = time.time() + self.HEARTBEAT_INTERVAL
            if socks.get(self.frontend) == zmq.POLLIN:
                frames = self.frontend.recv_multipart()
                if not frames:
                    break

                frames.insert(0, self.next())
                self.backend.send_multipart(frames)

            self.purge()
            
    def purge(self):
        """Look for & kill expired workers."""
        t = time.time()
        expired = []
        for address,worker in self.queue.iteritems():
            if t > worker.expiry:  # Worker expired
                expired.append(address)
        for address in expired:
            logging.info("[WORKER]: Idle worker expired: %s" % address)
            self.queue.pop(address, None)

    def next(self):
        address, worker = self.queue.popitem(False)
        return address

if __name__ == "__main__":
    queue = WorkerQueue()
    queue.connect()
    queue.run()