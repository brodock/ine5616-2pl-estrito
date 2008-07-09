#!/usr/bin/env python
# -*- coding: UTF-8 -*-

class DataManager(object):

    def __init__(self, log):
        # initialize database
        self.database = {
            'Registro1':1000,
            'Registro2':2000,
            'Registro3':3000,
            'Registro4':4000,
            'Registro5':2500}
        self.log = log
        self.data_changed = False

    # sgbd functions
    def read(self, tx, key, var):
        '''Read a register from database'''
        self.log.show_data('reading data', tx.id, key, self.database[key])
        tx.set_value(var, self.database[key])

    def write(self, tx, key, operation):
        '''Write a register to database'''
        value = tx.get_value(operation)
        self.log.show_data('writing data', tx.id, key, value)
        self.database[key] = value
        self.data_changed = True
