#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time
import thread
import random
from threading import Thread


class Transaction(object):

    def __init__(self, commands):
        self.id = hash(self)
        self.commands = commands
        self.timestamp = time.time()

    def get_value(var, operation):
        exec('v = ' + operation)
        return v

    def set_value(self, var, value):
        exec('self.%s = value' % var)


class TransactionManager(Thread):

    def __init__(self, LM, DM, log, timeout=3):
        Thread.__init__(self)
        self.queue = []
        self.log = log
        self.LM = LM
        self.DM = DM
        self.timeout = timeout
        self.running = True
        print 'TransactionManager loaded!'

    def schedule(self, transaction):
        '''Lock transaction commands and apply if successful, otherwise re-queue'''
        self.log.show('executando', transaction.id)
        self.log.write('start', transaction.id)

        locking_ok = True

        # phase 1
        # get all locks
        for cmd in transaction.commands:

            # transaction command example: ("read", "registro1", "x")
            action = cmd[0]
            data_item = cmd[1]

            if action == 'read':
                get_lock = self.LM.shared_lock
            else:
                get_lock = self.LM.exclusive_lock

            # deadlock detection
            # uses transaction timestamp to check for timeout
            # does not prevent livelocks
            lock_ok = False
            while (not lock_ok and time.time() - transaction.timestamp < self.timeout):
                # try to get lock for command
                lock_ok = get_lock(transaction.id, data_item)

                if lock_ok:
                    self.log.write('%s-lock(%s)' % (action, data_item), transaction.id)
                else:
                    # wait and try to get lock again
                    self.log.write('wait %s-lock(%s)' % (action, data_item), transaction.id)
                    time.sleep(0.5)

            if not lock_ok:
                # locking failed on last command, timeout detected
                # abort locking
                self.log.show('detectado deadlock no comando %s(%s)' % (action, data_item), transaction.id)
                self.log.write('timeout %s-lock(%s)' % (action, data_item), transaction.id)
                locking_ok = False
                break
            else:
                # update timestamp
                transaction.timestamp = time.time()

        if not locking_ok:
            # failed getting locks for transaction
            # re-schedule transaction
            self.log.show('abortando', transaction.id)
            self.log.write('abort', transaction.id)
            self.append(transaction)
            return False

        self.log.show('conseguiu todos os locks', transaction.id)

        # done locking, proceeding to phase 2
        # execute commands
        for cmd in transaction.commands:
            action = cmd[0]
            data_item = cmd[1]
            user_op = cmd[2]

            # send command to DataManager for execution
            getattr(self.DM, action)(transaction, data_item, user_op)
            self.log.write('%s(%s)' % (action, data_item), transaction.id)

            if action == 'read':
                # read locks can be released earlier
                self.LM.shared_unlock(transaction.id, data_item)
                self.log.write('%s-unlock(%s)' % (action, data_item), transaction.id)

        # all transaction commands finished
        #if commit_ok:
        #    DM.commit(transaction.id)
        #else:
        #    DM.rollback(transaction.id)
        self.log.write('commit', transaction.id)
        self.LM.unlock_all(transaction.id)
        self.log.write('unlock all', transaction.id)

    def append(self, transaction):
        '''Add transaction to queue'''
        self.log.show('adicionada ao scheduler', transaction.id)
        self.queue.append(transaction)

    def next_tx(self):
        '''Get next transaction in queue, None if queue is empty'''
        tx = None
        if self.queue:
            tx = self.queue.pop(0)
        return tx

    def run(self):
        '''Process transactions in queue'''
        while self.running:
            tx = self.next_tx()
            if tx:
                tx.timestamp = time.time()
                thread.start_new_thread(self.schedule, (tx,))
            time.sleep(0.5 + random.randrange(1))
