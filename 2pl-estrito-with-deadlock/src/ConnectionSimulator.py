#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from TransactionManager import TransactionManager, Transaction
from DataManager import DataManager
from LockManager import LockManager
from Logging import Log
import time


class ConnectionSimulator(object):
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
        print "ConnectionSimulator loaded!"

    def run(self):
        for t in self.transactions:
                trans = Transaction(t)
                TM.append(trans)
        while self.running:
            try:
                time.sleep(1)
            except KeyboardInterrupt, ex:
                TM.running = False
                log.running = False
                self.running = False


if __name__ == '__main__':
    print "Starting demonstration aplication to strict 2pl with deadlock solving"
    log = Log()
    DM = DataManager(log)
    LM = LockManager(log)
    TM = TransactionManager(LM, DM, log)
    CM = ConnectionSimulator()
    log.DM = DM
    log.start()
    TM.start()
    CM.run()
