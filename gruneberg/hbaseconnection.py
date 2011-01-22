from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

'''
Wrapper class for an HBase thrift connection
'''
class HBaseConnection:
    def __init__(self, server_name, port):    
        # Make socket
        self.transport = TSocket.TSocket(server_name, port)
        
        # Buffering is critical. Raw sockets are very slow
        self.transport = TTransport.TBufferedTransport(self.transport)
        
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(protocol)
        print "Opening HBase Connection"
        self.transport.open()

    def get_client(self):
        return self.client
        
    def close(self):
        print "Closing HBase Connection"
        self.transport.close()

hbc = HBaseConnection("hp-node01.phx1.mozilla.com", 9090)
client = hbc.get_client()
table_name = "crash_reports_index_legacy_unprocessed_flag"
scanner_id = client.scannerOpen(table_name, "", [])
raw_row = client.scannerGet(scanner_id)
count = 0
while raw_row:
    count += 1
    raw_row = client.scannerGet(scanner_id)
    
client.scannerClose(scanner_id)
print count