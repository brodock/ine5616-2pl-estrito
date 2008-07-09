#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time
from threading import Thread
from LockManager import LockManager
from DataManager import DataManager

class Transaction(Thread):
    
    def __init__(self, commands):
        Thread.__init__(self)
        self.id = hash(self)
        self.commands = commands
        

class TransactionManager(object):
    
    def __init__(self):
        self.queue = []
        self.running = True
    
    def parse(self, transaction):
        '''Lock transaction commands and apply if successful, otherwise re-queue'''
        commit_ok = True
        for cmd in transaction.commands:
            if not LM.lock(transaction.id, cmd[1], cmd[0]):
                # could not get lock, try again
                self.append(transaction)
                commit_ok = False
                break
            else:
                # got lock, execute command
                getattr(DM, cmd[0])(cmd[1], cmd[2])
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
            
if __name__ == '__main__':
    print "Iniciando aplicação de demonstração do 2pl estrito com tratamento de deadlock"
    TM = TransactionManager()
    LM = LockManager()
    DM = DataManager()
    tx1_cmd = [
        ('read', 'Registro1', 'x'),
        ('read', 'Registro2', 'y'),
        ('write', 'Registro1', 'x+y')]
    tx1 = Transaction(tx1_cmd)
    TM.append(tx1)