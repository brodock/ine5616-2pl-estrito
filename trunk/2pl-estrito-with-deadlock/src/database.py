#!/usr/bin/env python
# -*- coding: UTF-8 -*-

class Database(object):

    def __init__(self):
        '''Initialize database'''
        self.database = {
            'Registro1':1000,
            'Registro2':2000,
            'Registro3':3000,
            'Registro4':4000}
    
    # sgbd functions
    def read(self, key):
        '''Read a register from database'''
        return self.database[key]
        
    def write(self, key, value):
        '''Write a register to database'''
        self.database[key] = value
    
    def parse(self, commands):
        '''Parse all transaction commands'''
        for cmd in commands:
            exec(cmd)
