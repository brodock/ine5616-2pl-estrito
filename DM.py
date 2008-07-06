class DataManager():
	
	def __init__(self):
	    # initialize database
        self.database = {
            'A':1000,
            'B':2000,
            'C':3000,
            'D':4000,
            'E':2500}
    
    # sgbd functions
    def read(self, key):
        '''Read a register from database'''
        return self.database[key]
        
    def write(self, key, value):
        '''Write a register to database'''
        self.database[key] = value
    
    def parse(commands):
        '''Parse all transaction commands'''
        for cmd in commands:
            exec(cmd)