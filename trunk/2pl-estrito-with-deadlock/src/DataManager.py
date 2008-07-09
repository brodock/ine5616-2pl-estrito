#!/usr/bin/env python
# -*- coding: UTF-8 -*-

class DataManager(object):
    
    def __init__(self):
        # initialize database
        self.database = {
            'Registro1':1000,
            'Registro2':2000,
            'Registro3':3000,
            'Registro4':4000,
            'Registro5':2500}
        print "DataManager carregado com sucesso!"
        self.data_changed = False
    
    # sgbd functions
    def read(self, tx, key, var):
        '''Read a register from database'''
        print "[T%s] Fazendo leitura do registro %s (%s)" % (tx.id, key, self.database[key])
        tx.set_value(var, self.database[key])
        
    def write(self, tx, key, operation):
        '''Write a register to database'''
        value = tx.get_value(operation)
        print "[T%s] Escrevendo no registro %s = %s" % (tx.id, key, value)
        self.database[key] = value
        self.data_changed = True
