#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time


class LockManager(object):
    
    def __init__(self):
        '''
        The lock table supports the following format:
        {reg: {Tx: {'S': n} } }
        or
        {reg: {Tx: {'X': 1} } }
        
        '''
        self.lock_table = {}
        print "LockManager carregado com sucesso!"
    
    def shared_lock(self, tx_id, data_item):
        '''Acquire shared lock (read) on a data item'''
        lock_type = self.check_lock(data_item)
        if lock_type == 'U':
            self.shared_list(tx_id, data_item)
            print '[T%s] got shared lock on %s (was Unlocked)' % (tx_id, data_item)
            return True
        elif lock_type == 'S':
            self.shared_list(tx_id, data_item)
            print '[T%s] got shared lock on %s (was Shared)' % (tx_id, data_item)
            return True
        else:
            # lock_type == 'X'
            print '[T%s] cannot get shared lock on %s (type X active)' % (tx_id, data_item)
            return False
    
    def exclusive_lock(self, tx_id, data_item):
        '''Acquire exclusive lock (write) on a data item'''
        lock_type = self.check_lock(data_item)
        if lock_type == 'U':
            print '[T%s] got exclusive lock on %s (was Unlocked)' % (tx_id, data_item)
            print self.list_modes(data_item)
            self.exclusive_list(tx_id, data_item)
            return True
        elif lock_type == 'S':
            print '[T%s] trying exclusive lock on %s (is Shared)' % (tx_id, data_item)
            if self.lock_table[data_item].has_key(tx_id):
                if len(self.lock_table[data_item]) == 1:
                    # only we have shared lock on it, so upgrade is possible
                    print '[T%s] upgrading shared lock to exclusive lock on %s' % (tx_id, data_item)
                    self.exclusive_list(tx_id, data_item)
                    return True
                else:
                    print '[T%s] cannot upgrade lock on %s (Shared with many)' % (tx_id, data_item)
                    return False
            else:
                print '[T%s] cannot upgrade lock on %s (Holds no shares)' % (tx_id, data_item)
                return False
        else:
            # lock_type == 'X'
            if self.lock_table[data_item].has_key(tx_id):
                if len(self.lock_table[data_item]) == 1:
                    # we already have exclusive lock, so it's ok
                    print '[T%s] got exclusive lock on %s (already had X)' % (tx_id, data_item)
                    return True
            print '[T%s] cannot get exclusive lock on %s (other X active)' % (tx_id, data_item)
            return False
    
    def shared_list(self, tx_id, data_item):
        '''Add transaction to shared lock list on data item'''
        if not self.lock_table.has_key(data_item):
            self.lock_table[data_item] = {}
        lock_list = self.lock_table[data_item]
        if lock_list.has_key(tx_id):
            lock_list[tx_id]['S'] += 1
        else:
            lock_list[tx_id] = {'S': 1}
    
    def exclusive_list(self, tx_id, data_item):
        '''Add transaction to exclusive lock list on data item'''
        if not self.lock_table.has_key(data_item):
            self.lock_table[data_item] = {}
        if not self.lock_table[data_item].has_key(tx_id):
            self.lock_table[data_item][tx_id] = {'X': 1}
        self.lock_table[data_item][tx_id]['X'] = 1
    
    def check_lock(self, data_item):
        '''
        Return lock type over a data item:
        'U' for unlocked, 'S' for shared and 'X' for exclusive
        
        '''
        if not self.lock_table.has_key(data_item):
            return 'U'
        elif 'S' in self.list_modes(data_item):
            return 'S'
        elif 'X' in self.list_modes(data_item):
            return 'X'
        else:
            return 'U'
    
    def list_modes(self, data_item):
        '''List all lock modes for a data item'''
        values = [] 
        for tx, modes in self.lock_table[data_item].items():
            for m in modes:
                values.append(m)
        return values
    
    def shared_unlock(self, tx_id, data_item):
        '''Remove one shared lock from a data item'''
        self.lock_table[data_item][tx_id]['S'] -= 1
        if self.lock_table[data_item][tx_id]['S'] == 0:
            self.unlock(tx_id, data_item)
            
    def unlock(self, tx_id, data_item):
        '''Remove a transaction lock from a data item'''
        print "Removendo lock da %s em %s" % (tx_id, data_item)
        del(self.lock_table[data_item][tx_id])
        
    def unlock_all(self, tx_id):
        '''Remove all locks held by a transaction'''
        for data_item, txs in self.lock_table.items():
            if tx_id in txs:
                self.unlock(tx_id, data_item)
    