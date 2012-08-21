# -*- coding: utf-8 -*-
"""
Created on Sun Aug  5 00:14:40 2012

@author: Artem <mail@artem-shitov.com> Shitov
"""

import threading
import logging
import queue


class Caster(object):
    def __init__(self, maxThreads=4):
        self.__maxThreads = maxThreads
        self.__consumers = []
        self.__maxJobId = 0
        self.__maxJobIdLock = threading.Lock()
        self.__renewLock = threading.Lock()
        self.__threadPool = queue.Queue()
        self.__activePool = {}  # jobId => thread
        self.__activePoolLock = threading.Lock()

    def __endJob(self, jobId):
        self.__activePoolLock.acquire()
        try:
            self.__activePool[jobId]
            self.__activePool.pop(jobId)
            self.__renewActiveJobs()
        finally:
            self.__activePoolLock.release()

    def __pushJob(self, fn):
        self.__maxJobIdLock.acquire()
        try:
            jobId = self.__maxJobId
            self.__maxJobId += 1
        finally:
            self.__maxJobIdLock.release()
        self.__threadPool.put((jobId, fn))
        self.__renewActiveJobs()

    def __renewActiveJobs(self):
        self.__renewLock.acquire()
        try:
            while not self.__threadPool.empty() and \
               len(self.__activePool) < self.__maxThreads:
                jobId, fn = self.__threadPool.get()
                thread = threading.Thread(target=fn, kwargs={'jobId': jobId})
                self.__activePool[jobId] = thread
                thread.start()
        finally:
            self.__renewLock.release()

    def attach(self, consumer):
        if not consumer in self.__consumers:
            self.__consumers.append(consumer)
            return True
        else:
            return False

    def detach(self, consumer):
        if consumer in self.__consumers:
            self.__consumers.remove(consumer)
            return True
        else:
            return False

    def send(self, callback=None, *args, **kwargs):
        for consumer in self.__consumers:
            # a hack (entry=entry) to avoid lexical passing of object
            # see: http://stackoverflow.com/questions/233673
            def run(jobId=None, consumer=consumer):
                try:
                    result = consumer.consume(*args, **kwargs)
                    if callback != None:
                        callback(result)
                except Exception as e:
                    logging.error("Exception at consumer: %s" % str(e))
                    callback(e)
                finally:
                    self.__endJob(jobId)
            self.__pushJob(run)