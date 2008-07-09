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
    
    def __init__(self, LM, DM, timeout=2):
        Thread.__init__(self)
        self.queue = []
        self.running = True
        self.timeout = timeout
        self.LM = LM
        self.DM = DM
        print 'TransactionManager loaded!'
    
    def parse(self, transaction):
        '''Lock transaction commands and apply if successful, otherwise re-queue'''
        locking_ok = True
        # get locks
        for cmd in transaction.commands:
            # transaction line - "read", "registro1", "x"
            action = cmd[0]
            data_item = cmd[1]
            if action == 'read':
                get_lock = self.LM.shared_lock
            else:
                get_lock = self.LM.exclusive_lock
            
            # deadlock prevention
            # uses transaction timestamp to check for timeout
            lock_ok = False
            while (not lock_ok and time.time() - transaction.timestamp < self.timeout):
                lock_ok = get_lock(transaction.id, data_item)
                time.sleep(0.5)
            
            if not lock_ok:
                # locking failed on last command, abort
                print '!!!!!!! ----->>> [T%s] detectado deadlock no comando %s(%s)' % (transaction.id, action, data_item)
                locking_ok = False
                break
            else:
                # update timestamp
                transaction.timestamp = time.time()
                
        if not locking_ok:
            # failed getting locks for transaction
            # re-schedule transaction
            print "[T%s] aborting..." % transaction.id
            self.append(transaction)
            return False

        print "[T%s] I've got all the locks I need (Oh Yeah!)" % transaction.id
        # done locking, proceeding to phase 2
        # execute commands
        for cmd in transaction.commands:
            action = cmd[0]
            data_item = cmd[1]
            user_op = cmd[2]
            # send command to DataManager
            getattr(self.DM, action)(transaction, data_item, user_op)
            if action == 'read':
                # read locks can be released earlier
                self.LM.shared_unlock(transaction.id, data_item)
        
        # all transaction commands finished
        #if commit_ok:
        #    DM.commit(transaction.id)
        #else:
        #    DM.rollback(transaction.id)
        self.LM.unlock_all(transaction.id)
                
    def append(self, transaction):
        '''Add transaction to queue'''
        print 'Adicionando transação %s' % transaction.id
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
                print '[T%s] executando' % tx.id
                tx.timestamp = time.time()
                thread.start_new_thread(self.parse, (tx,))
                #self.parse(tx)
            time.sleep(random.randrange(1,4))
