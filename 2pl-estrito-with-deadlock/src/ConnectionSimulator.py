#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time
from threading import Thread
from TransactionManager import TransactionManager, Transaction
from LockManager import LockManager
from DataManager import DataManager

class ConnectionSimulator():
    def __init__(self):
        self.transactions = []
        self.running = True

        # every append adds one transaction that gonna be started as thread after
        transaction_cmd = [
        ('read', 'Registro2', 'x'),
        ('read', 'Registro3', 'y'),
        ('write', 'Registro3', 'var.x + var.y')]
        self.transactions.append(transaction_cmd)
        
        transaction_cmd = [
        ('read', 'Registro1', 'x'),
        ('read', 'Registro2', 'y'),
        ('write', 'Registro2', 'var.x + var.y')]
        self.transactions.append(transaction_cmd)
        
        transaction_cmd = [
        ('read', 'Registro5', 'x'),
        ('read', 'Registro4', 'y'),
        ('write', 'Registro4', 'var.x'),
        ('write', 'Registro5', 'var.y')]
        self.transactions.append(transaction_cmd)
        
        transaction_cmd = [
        ('read', 'Registro4', 'x'),
        ('read', 'Registro5', 'y'),
        ('write', 'Registro5', 'var.x + 99'),
        ('write', 'Registro4', 'var.y + 99')]
        
        self.transactions.append(transaction_cmd)
        print "ConnectionSimulator carregado com sucesso!"
    
    def run(self):
        for t in self.transactions:
                # starteia t
                trans = Transaction(t)
                TM.append(trans)
        while self.running:
            try:
                if DM.data_changed:
                    print "\nEstado atual dos dados:"
                    for d in DM.database:
                        print "%s = %s" % (d, DM.database[d])
                    print ""
                    DM.data_changed = False
                time.sleep(1)
            except KeyboardInterrupt, ex:
                self.running = False
            
                
if __name__ == '__main__':
    print "Iniciando aplicação de demonstração do 2pl estrito com tratamento de deadlock"
    LM = LockManager()
    DM = DataManager()
    TM = TransactionManager(LM, DM)
    TM.start()
    CM = ConnectionSimulator()
    CM.run()