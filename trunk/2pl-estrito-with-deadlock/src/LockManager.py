#!/usr/bin/env python
# -*- coding: UTF-8 -*-

class LockManager(object):
    
    def __init__(self):
        '''
        The lock table supports the following format:
        {reg: {Tx: {'S': n} } }
        or
        {reg: {Tx: {'W': 1} } }
        
        '''
        self.lock_table = {}
        print "LockManager carregado com sucesso!"
    
    def shared_lock(self, tx_id, data_item):
        '''Acquire shared lock (read) on a data item'''
        lock_type = self.check_lock(data_item)
        if lock_type == 'U':
            self.shared_list(tx_id, data_item)
        elif lock_type == 'S':
            self.shared_list(tx_id, data_item)
        else:
            # lock_type == 'X'
            return
            wait() #TODO
    
    def exclusive_lock(self, tx_id, data_item):
        '''Acquire exclusive lock (write) on a data item'''
        lock_type = self.check_lock(data_item)
        if lock_type == 'U':
            self.exclusive_list(tx_id, data_item)
        elif lock_type == 'S':
            if self.lock_table[data_item].has_key(tx_id):
                if len(self.lock_table[data_item]) == 1:
                # only we have shared lock on it, so upgrade is possible
                self.exclusive_list(tx_id, data_item)
            else:
                return
                wait() #TODO
        else:
            # lock_type == 'X'
            return
            wait() #TODO
    
    def shared_list(self, tx_id, data_item):
        '''Add transaction to shared lock list on data item'''
        lock_list = self.lock_table[data_item]
        if lock_list.has_key(tx_id):
            lock_list[tx_id]['S'] += 1
        else:
            lock_list[tx_id] = {'S': 1}
    
    def exclusive_list(self, tx_id, data_item):
        '''Add transaction to exclusive lock list on data item'''
        self.lock_table[data_item][tx_id] = {'W': 1}
    
    def lock_type(self, data_item):
        '''
        Return lock type over a data item:
        'U' for unlocked, 'S' for shared and 'X' for exclusive
        
        '''
        if not self.lock_table.has_key(data_item):
            return 'U'
        elif 'S' in list_modes(data_item):
            return 'S'
        else:
            return 'X'
    
    def list_modes(data_item):
        '''List all lock modes for a data item'''
        locks = self.lock_table[data_item]
        modes = [locks[tx] for tx in locks]
        return modes
    
    def unlock(self, tx_id, data_item):
        '''Remove a transaction lock from a data item'''
        print "Removendo lock da %s " % tx_id
        del(self.lock_table[data_item][tx_id])
        
    def unlock_all(self, tx_id):
        '''Remove all locks held by a transaction'''
        for data_item, txs in self.lock_table.items():
            if tx_id in txs:
                self.unlock(tx_id, data_item)