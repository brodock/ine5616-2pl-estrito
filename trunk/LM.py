class LockManager(object):
    
    def __init__(self):
        self.lock_table = {}
     
    def lock(self, tx_id, data_item, mode):
        '''
        Acquire a lock, returning whether succeeded or not
        WARNING: the following method is a brainfuck
        '''
        if self.lock_table.has_key(data_item):
            # our data item already has locks on it
            if 'write' in self.list_modes(data_item):
                # there is a write lock
                if mode == 'write' and tx_id in self.lock_table[data_item]:
                    # we have the write lock and are requesting again
                    # this is ok
                    return True
                else:
                    # we are either requesting a read on a write locked data item
                    # or another transaction holds the write lock
                    # this is not ok
                    return False
            else:
                # there are only read locks
                    if mode == 'write':
                        if self.lock_table[data_item].has_key(tx_id):
                            # we have a read lock and want to make it to a
                            # write lock, so we are requesting a lock upgrade
                            return self.upgradelock(tx_id, data_item)
                        else:
                            # cannot write lock because other transaction
                            # hold the read locks
                            return False
                    else:
                        # already have read lock or getting new one
                        # either way, it works
                        self.lock_table[data_item][tx_id] = mode
                        return True
        else:
            # data item has no locks yet
            # so we grant any lock
            self.lock_table[data_item] = {tx_id: mode}
            return True
                
    def list_modes(data_item):
        '''List all lock modes for a data item'''
        locks = self.lock_table[data_item]
        modes = [locks[tx] for tx in locks]
        return modes
        
    def upgradelock(self, tx_id, data_item):
        '''Upgrade a read lock to a write lock'''
        if len(self.lock_table[data_item]) == 1:
            # only we have lock on it, so upgrade is possible
            self.lock_table[data_item][tx_id] == 'write'
            return True
        else:
            # cannot upgrade since other transactions
            # are locking same data item
            return False
            
    def unlock(self, tx_id, data_item):
        '''Remove a transaction lock from a data item'''
        del(self.lock_table[data_item][tx_id])
        
    def unlock_all(self, tx_id):
        '''Remove all lock held by a transaction'''
        for data_item, txs in self.lock_table.items():
            if tx_id in txs:
                self.unlock(tx_id, data_item)