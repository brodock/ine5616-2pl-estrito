#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time
from threading import Thread

class Transaction(Thread):
    
    def __init__(self, commands):
        Thread.__init__(self)
        self.id = hash(self)
        self.commands = commands
        print "[T%s] Transação iniciada!" % self.id
        

class TransactionManager(Thread):
    
    def __init__(self):
        Thread.__init__(self)
        self.queue = []
        self.running = True
        self.tx_table = {}
        print "TransactionManager carregado com sucesso!"
    
    def parse(self, transaction):
        '''Lock transaction commands and apply if successful, otherwise re-queue'''
        commit_ok = True
        # get locks
        for cmd in transaction.commands:
            # transaction line - "read", "registro1", "x"
            action = cmd[0]
            data_item = cmd[1]
            LM.lock(transaction.id, action, data_item)
            print "[T%s] conseguiu todos os locks" % transaction.id
            # done locking, proceeding to phase 2
        # execute commands
        for cmd in transaction.commands:
            action = cmd[0]
            data_item = cmd[1]
            user_op = cmd[2]
            # send command to DataManager
            getattr(DM, action)(transaction.id, data_item, user_op)
            if action == 'read':
                # read locks can be released earlier
                LM.unlock(transaction.id, data_item)
        # 
        if commit_ok:
            DM.commit(transaction.id)
        else:
            DM.rollback(transaction.id)
        LM.unlockall(transaction.id)
                
    def append(self, transaction):
        '''Add transaction to queue'''
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
                self.parse(tx)
            time.sleep(0.5)
