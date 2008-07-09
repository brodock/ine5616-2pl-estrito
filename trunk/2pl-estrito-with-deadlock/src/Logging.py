#!/usr/bin/env python
#-*- coding: utf-8 -*-

from threading import Thread
import time


class Log (Thread):

    def __init__(self, name='2pl_estrito'):
        Thread.__init__(self)
        self.DM = None
        self.log = '%s.log' % name
        open(self.log, 'w').write('')
        self.running = True

    def write(self, msg, tx_id):
        f = open(self.log, 'a')
        f.write('<T%s %s>\n' % (str(tx_id)[-2:], msg))
        f.close()

    def show(self, msg, tx_id):
        print '[T%s] %s' % (str(tx_id)[-2:], msg)

    def show_lock(self, msg, tx_id, data_item):
        print '[T%s] {%s}: %s' % (str(tx_id)[-2:], data_item, msg)

    def show_data(self, msg, tx_id, data_item, value):
        print '[T%s] {%s}: %s: %s' % (str(tx_id)[-2:], data_item, msg, value)

    def run(self):
        while self.running:
            if self.DM.data_changed:
                print '\nEstado atual dos dados:'
                for k,v in self.DM.database.items():
                    print '%s = %s' % (k, v)
                print ''
                self.DM.data_changed = False
            time.sleep(0.5)
