#!/usr/bin/env python

import thread
import time


# initialize database
database = {
    'Registro1':1000,
    'Registro2':2000,
    'Registro3':3000,
    'Registro4':4000}

#transactions
Tx1 = [
    "x = read('Registro1')",
    "y = read('Registro2')",
    "write('Registro1', x + y)"]

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
    
# execute transactions
print database
thread.start_new_thread(parse, (Tx1,))
time.sleep(1)
print database
