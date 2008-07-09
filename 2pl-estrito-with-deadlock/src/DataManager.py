#!/usr/bin/env python
# -*- coding: UTF-8 -*-

class DataManager():
    
    def __init__(self):
        # initialize database
        self.database = {
            'Registro1':1000,
            'Registro2':2000,
            'Registro3':3000,
            'Registro4':4000,
            'Registro5':2500}
        print "DataManager carregado com sucesso!" 
    
    # sgbd functions
    def read(self, tx_id, key, var):
        '''Read a register from database'''
        print "Fazendo leitura do registro %s (%s)" % (key, self.database[key])
        return self.database[key]
        
    def write(self, key, value):
        '''Write a register to database'''
        print "Escrevendo no registro %s = %s" % (key, value)
        self.database[key] = value
    
    def parse(commands):
        '''Parse all transaction commands'''
        for cmd in commands:
            exec(cmd)
