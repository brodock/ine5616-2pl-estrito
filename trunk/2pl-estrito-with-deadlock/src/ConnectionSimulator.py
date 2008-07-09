#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time
from threading import Thread
from TransactionManager import TransactionManager, Transaction
from LockManager import LockManager
from DataManager import DataManager

class ConnectionSimulator(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.transactions = []
        self.running = True

        # cada append adiciona uma transação que vai ser startada como threads depois
        transaction_cmd = [
        ('read', 'Registro1', 'x'),
        ('read', 'Registro2', 'y'),
        ('write', 'Registro1', 'x+y')]
        self.transactions.append(transaction_cmd)
        print "ConnectionSimulator carregado com sucesso!"
    
    def run(self):
        while self.running:
            for t in self.transactions:
                # starteia t
                trans = Transaction(t)
                TM.append(trans)
            time.sleep(2.5)
                
if __name__ == '__main__':
    print "Iniciando aplicação de demonstração do 2pl estrito com tratamento de deadlock"
    TM = TransactionManager()
    LM = LockManager()
    DM = DataManager()
    CM = ConnectionSimulator()
    CM.start()