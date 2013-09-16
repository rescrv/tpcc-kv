# Copyright (c) 2014-2015, Robert Escriva
# All rights reserved.

import time

from ygor import Environment
from ygor import Experiment
from ygor import Host
from ygor import HostSet
from ygor import Parameter
from ygor import Utility


class TPCCKV(Experiment):

    # Parameters
    SYSTEM = Parameter('consus')
    WAREHOUSES = Parameter(10)
    DISTRICTS = Parameter(10)
    CLIENTS_PER_DISTRICT = Parameter(1)
    OPERATIONS = Parameter(100000)

    # Environment
    CONSUS_HOST = Environment('localhost')
    CONSUS_PORT = Environment('1982')

    # Hosts
    CLIENTS = HostSet('clients')

    @Utility
    def initialize(self):
        self.CLIENTS.run_many(('tpcc-kv', 'setup', self.SYSTEM) + self.db_args(),
                              number=1)

    @Utility
    def load(self):
        W = self.WAREHOUSES.as_int()
        D = self.DISTRICTS.as_int()
        self.CLIENTS.run_many(('tpcc-kv', 'load-items', self.SYSTEM,
                               '--warehouses', self.WAREHOUSES) + self.db_args(),
                              number=1)
        self.CLIENTS.run_many(('tpcc-kv', 'load-warehouse', self.SYSTEM,
                               '--warehouses', self.WAREHOUSES,
                               '--warehouse', HostSet.Index(lambda x: x + 1)) + self.db_args(),
                              number=W)
        self.CLIENTS.run_many(('tpcc-kv', 'load-district', self.SYSTEM,
                               '--warehouses', W, '--districts', D,
                               '--warehouse', HostSet.Index(lambda x: (x % W) + 1),
                               '--district', HostSet.Index(lambda x: ((x // W) % D) + 1)) + self.db_args(),
                              number=W * D * self.CLIENTS_PER_DISTRICT.as_int())

    @Utility
    def wipe(self):
        self.CLIENTS._manyrun(('tpcc-kv', 'wipe', self.SYSTEM), number=1)

    def all(self):
        return self._common()

    def new_order(self):
        return self._common(('--new-order-only',))

    @Utility
    def _common(self, args=()):
        W = self.WAREHOUSES.as_int()
        D = self.DISTRICTS.as_int()
        self.CLIENTS.run(('mkdir', '-p', 'data'))
        num_clients = W * D * self.CLIENTS_PER_DISTRICT.as_int()
        self.CLIENTS.run_many(('tpcc-kv', 'run', self.SYSTEM,
                               '--operations', HostSet.Index(lambda x: self.OPERATIONS.as_int() // num_clients),
                               '--output', HostSet.Index(lambda x: ('data/' + str(self.SYSTEM) + '-{0}.dat'.format(x))),
                               '--warehouses', W, '--districts', D,
                               '--warehouse', HostSet.Index(lambda x: (x % W) + 1),
                               '--district', HostSet.Index(lambda x: ((x // W) % D) + 1)) + self.db_args() + args,
                              number=num_clients)
        self.CLIENTS.collect('tpcc.dat', HostSet.Index(lambda x: ('data/' + str(self.SYSTEM) + '-{0}.dat'.format(x))),
                              number=num_clients)

    def db_args(self):
        return ('--host', self.CONSUS_HOST, '--port', self.CONSUS_PORT)
