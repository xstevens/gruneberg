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
        self.transport.open()

    def get_client(self):
        return self.client
        
    def close(self):
        self.transport.close()