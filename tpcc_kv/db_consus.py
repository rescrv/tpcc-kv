# Copyright (c) 2016
# All rights reserved.

import consus

import tpcc_kv

class Database(tpcc_kv.Database):

    ATOMIC = False

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = consus.Client(host, port)
        self.xact = None

    def error(self):
        assert False # XXX

    def setup(self):
        pass # XXX

    def wipe(self):
        pass # XXX

    def begin_transaction(self):
        assert self.xact is None
        self.xact = self.client.begin_transaction()

    def commit_transaction(self):
        self.xact.commit()
        self.xact = None

    def abort_transaction(self):
        self.xact.abort()
        self.xact = None

    def _get_warehouse(self, key):
        return self.get('WAREHOUSE', self.encode(key))

    def _store_warehouse(self, key, warehouse):
        return self.put('WAREHOUSE', self.encode(key), warehouse)

    def _get_district(self, key):
        return self.get('DISTRICT', self.encode(key))

    def _store_district(self, key, district):
        return self.put('DISTRICT', self.encode(key), district)

    def _get_customer(self, key):
        return self.get('CUSTOMER', self.encode(key))

    def _store_customer(self, key, customer):
        return self.put('CUSTOMER', self.encode(key), customer)

    def _store_new_order(self, key, new_order):
        return self.put('NEW_ORDER', self.encode(key), new_order)

    def _get_order(self, key):
        return self.get('ORDER', self.encode(key))

    def _store_order(self, key, order):
        return self.put('ORDER', self.encode(key), order)

    def _get_order_line(self, key):
        return self.get('ORDER_LINE', self.encode(key))

    def _store_order_line(self, key, order_line):
        return self.put('ORDER_LINE', self.encode(key), order_line)

    def _get_item(self, key):
        return self.get('ITEM', self.encode(key))

    def _store_item(self, key, item):
        return self.put('ITEM', self.encode(key), item)

    def _get_stock(self, key):
        return self.get('STOCK', self.encode(key))

    def _store_stock(self, key, stock):
        return self.put('STOCK', self.encode(key), stock)

    def _store_history(self, key, history):
        return self.put('HISTORY', self.encode(key), history)

    def get(self, space, key):
        space = space.upper()
        if self.xact is None: return self.xact_get(space, key)
        return self.xact.get(space, key)

    def xact_get(self, space, key):
        try:
            self.begin_transaction()
            return self.get(space, key)
        finally:
            self.commit_transaction()

    def put(self, space, key, value):
        space = space.upper()
        if self.xact is None: return self.xact_put(space, key, value)
        return self.xact.put(space, key, value)

    def xact_put(self, space, key, value):
        try:
            self.begin_transaction()
            return self.put(space, key, value)
        finally:
            self.commit_transaction()

    def encode(self, key):
        return str(tuple(key)).replace('L', '')

def add_arguments(parser):
    parser.add_argument('--host', type=str, default='127.0.0.1')
    parser.add_argument('--port', type=int, default=1982)

def create_database(args):
    return Database(args.host, args.port)
