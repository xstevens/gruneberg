#!/usr/bin/env python

# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0/LGPL 2.1
#
# The contents of this file are subject to the Mozilla Public License Version
# 1.1 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
# for the specific language governing rights and limitations under the
# License.
#
# The Original Code is Mozilla Foundation.
#
# The Initial Developer of the Original Code is
# Xavier Stevens <xstevens@mozilla.com>.
# Portions created by the Initial Developer are Copyright (C) 2011
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#
# Alternatively, the contents of this file may be used under the terms of
# either the GNU General Public License Version 2 or later (the "GPL"), or
# the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
# in which case the provisions of the GPL or the LGPL are applicable instead
# of those above. If you wish to allow use of your version of this file only
# under the terms of either the GPL or the LGPL, and not to allow others to
# use your version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the notice
# and other provisions required by the GPL or the LGPL. If you do not delete
# the provisions above, a recipient may use your version of this file under
# the terms of any one of the MPL, the GPL or the LGPL.
#
# ***** END LICENSE BLOCK *****

import sys
import getopt
import struct
import logging
import re
from datetime import datetime, timedelta

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *
from hbaseconnection import HBaseConnection

from gmetric import GmetricConfig, Gmetric

# Setup logging facilities
log = logging.getLogger("")
log.setLevel(logging.DEBUG)

'''
A class to emit Ganglia metrics based on values in HBase tables
'''
class HBaseMetric:
    def __init__(self, hbc, gmetric):
        self.hbc = hbc
        self.gmetric = gmetric
        
    def emit_row(self, table_name, row_id, gmetric_config):
        results = self.hbc.get_client().getRow(table_name, row_id)
        result_dict = {}
        for r in results:
            for column_name,v in r.columns.items():
                result_dict[column_name] = long(struct.unpack('>q', v.value)[0])

        for column_name,column_value in result_dict.iteritems():
            log.debug("%s => %s" % (column_name, column_value))
            metric_name = "%s.%s" % (table_name, column_name)
            self.gmetric.send_meta(metric_name, gmetric_config.type, gmetric_config.units, gmetric_config.slope, gmetric_config.tmax, gmetric_config.dmax, gmetric_config.group_name)
            self.gmetric.send(metric_name, column_value, gmetric_config.type)
        
'''
Print usage information
'''
def usage():
    print "Usage: %s [--help] [--hbase_thrift server[:port]] [--gmetric server[:port]] table row_id" % (sys.argv[0])
    
def main(argv=None):
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "hbase_thrift=", "gmetric=", "group="])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    
    thrift_server = "localhost"
    thrift_server_port = 9090
    ganglia_host = "localhost"
    ganglia_port = 8649
    ganglia_protocol = "multicast"
    ganglia_metic_group_name = "hbase"
    # process options
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif o == "--hbase_thrift":
            splits = a.split(":")
            thrift_server = splits[0]
            if len(splits) == 2:
                thrift_server_port = splits[1]
        elif o == "--gmetric":
            splits = a.split(":")
            ganglia_host = splits[0]
            if len(splits) == 2:
                ganglia_port = splits[1]
        elif o == "--group":
            ganglia_metric_group_name = a
                
    # process arguments
    row_id = None
    table_name = None
    if len(args) > 1:
        table_name = args[0]
        row_id = args[1]
    
    hbc = None
    try:
        hbc = HBaseConnection(thrift_server, thrift_server_port)
        gmetric = Gmetric(ganglia_host, ganglia_port, ganglia_protocol)
        hbm = HBaseMetric(hbc, gmetric)
        gmc = GmetricConfig(type="int32", group_name=ganglia_metric_group_name)
        hbm.emit_row(table_name, row_id, gmc)                
    except Thrift.TException, tx:
        logging.error("Thrift exception: %s" % (tx.message))
    finally:
        if hbc:
            hbc.close()
    
if __name__ == "__main__":
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(console)
    sys.exit(main())