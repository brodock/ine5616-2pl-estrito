#!/usr/bin/env python

class database():
    # initialize database
    database = {
        'A':1000,
        'B':2000,
        'C':3000,
        'D':4000}
    
    # sgbd functions
    def read(key):
        '''Read a register from database'''
        return database[key]
        
    def write(key, value):
        '''Write a register to database'''
        database[key] = value
    
    def parse(commands):
        '''Parse all transaction commands'''
        for cmd in commands:
            exec(cmd)