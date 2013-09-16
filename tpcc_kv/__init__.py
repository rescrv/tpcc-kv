# Copyright (c) 2013-2017
# All rights reserved.

import abc
import collections
import inspect
import itertools
import os
import random
import string
import sys
import time

import argparse
import importlib
import ygor.collect

# This is a binding for running TPC-C on top of key-value stores.
# It boils TPC-C down to loads and stores of objects.  Simply implement a
# datastore that does get/put of the requested objects, the harness does the
# rest
#
# Deviations from TPC-C:
#  - The delivery transaction is skipped.  It'd normally be handled by a work
#    queue outside of the database.  All data required for the transaction
#    (e.g., NEW-ORDER entries) are kept, just not updated.
#  - Selecting customers by non-primary key has been replaced in favor of always
#    selecting by customer identifier.  This maximizes compatibility
#  - There may be bugs.  It's 100 pages of robot text.

ITEM_FIELDS = {'I_ID', 'I_IM_ID', 'I_NAME', 'I_PRICE', 'I_DATA'}
STOCK_FIELDS = {'S_I_ID', 'S_W_ID', 'S_QUANTITY', 'S_DIST_01', 'S_DIST_02',
        'S_DIST_03', 'S_DIST_04', 'S_DIST_05', 'S_DIST_06', 'S_DIST_07',
        'S_DIST_08', 'S_DIST_09', 'S_DIST_10', 'S_YTD', 'S_ORDER_CNT',
        'S_REMOTE_CNT', 'S_DATA'}
WAREHOUSE_FIELDS = {'W_ID', 'W_NAME', 'W_STREET_1', 'W_STREET_2', 'W_CITY',
        'W_STATE', 'W_ZIP', 'W_TAX', 'W_YTD'}
DISTRICT_FIELDS = {'D_ID', 'D_W_ID', 'D_NAME', 'D_STREET_1', 'D_STREET_2',
        'D_CITY', 'D_STATE', 'D_ZIP', 'D_TAX', 'D_YTD', 'D_NEXT_O_ID'}
CUSTOMER_FIELDS = {'C_ID', 'C_D_ID', 'C_W_ID', 'C_O_ID', 'C_FIRST', 'C_MIDDLE',
        'C_LAST', 'C_STREET_1', 'C_STREET_2', 'C_CITY', 'C_STATE', 'C_ZIP',
        'C_PHONE', 'C_SINCE', 'C_CREDIT', 'C_CREDIT_LIM', 'C_DISCOUNT',
        'C_BALANCE', 'C_YTD_PAYMENT', 'C_PAYMENT_CNT', 'C_DELIVERY_CNT',
        'C_DATA'}
HISTORY_FIELDS = {'H_C_ID', 'H_C_D_ID', 'H_C_W_ID', 'H_D_ID', 'H_W_ID',
        'H_DATE', 'H_AMOUNT', 'H_DATA'}
ORDER_FIELDS = {'O_ID', 'O_D_ID', 'O_W_ID', 'O_C_ID', 'O_ENTRY_D',
        'O_CARRIER_ID', 'O_OL_CNT', 'O_ALL_LOCAL'}
ORDER_LINE_FIELDS = {'OL_O_ID', 'OL_D_ID', 'OL_W_ID', 'OL_NUMBER', 'OL_I_ID',
        'OL_SUPPLY_W_ID', 'OL_DELIVERY_D', 'OL_QUANTITY', 'OL_AMOUNT',
        'OL_DIST_INFO'}
NEW_ORDER_FIELDS = {'NO_O_ID', 'NO_D_ID', 'NO_W_ID'}

class Database(object, metaclass=abc.ABCMeta):

    def __init__(self):
        pass

    @abc.abstractmethod
    def begin_transaction(self):
        pass

    @abc.abstractmethod
    def commit_transaction(self):
        pass

    @abc.abstractmethod
    def abort_transaction(self):
        pass

    def get_warehouse(self, key):
        warehouse = self._get_warehouse(key)
        assert set(warehouse.keys()).issuperset(WAREHOUSE_FIELDS)
        return warehouse

    @abc.abstractmethod
    def _get_warehouse(self, key):
        pass

    def store_warehouse(self, key, warehouse):
        assert set(warehouse.keys()).issuperset(WAREHOUSE_FIELDS)
        return self._store_warehouse(key, warehouse)

    @abc.abstractmethod
    def _store_warehouse(self, key, warehouse):
        pass

    def get_district(self, key):
        district = self._get_district(key)
        assert set(district.keys()).issuperset(DISTRICT_FIELDS)
        return district

    @abc.abstractmethod
    def _get_district(self, key):
        pass

    def store_district(self, key, district):
        assert set(district.keys()).issuperset(DISTRICT_FIELDS)
        return self._store_district(key, district)

    @abc.abstractmethod
    def _store_district(self, key, district):
        pass

    def get_customer(self, key):
        customer = self._get_customer(key)
        assert set(customer.keys()).issuperset(CUSTOMER_FIELDS)
        return customer

    @abc.abstractmethod
    def _get_customer(self, key):
        pass

    def store_customer(self, key, customer):
        assert set(customer.keys()).issuperset(CUSTOMER_FIELDS)
        return self._store_customer(key, customer)

    @abc.abstractmethod
    def _store_customer(self, key, customer):
        pass

    def store_new_order(self, key, new_order):
        assert set(new_order.keys()).issuperset(NEW_ORDER_FIELDS)
        return self._store_new_order(key, new_order)

    @abc.abstractmethod
    def _store_new_order(self, key, new_order):
        pass

    def get_order(self, key):
        order = self._get_order(key)
        assert set(order.keys()).issuperset(ORDER_FIELDS)
        return order

    @abc.abstractmethod
    def _get_order(self, key):
        pass

    def store_order(self, key, order):
        assert set(order.keys()).issuperset(ORDER_FIELDS)
        return self._store_order(key, order)

    @abc.abstractmethod
    def _store_order(self, key, order):
        pass

    def get_order_line(self, key):
        order_line = self._get_order_line(key)
        assert set(order_line.keys()).issuperset(ORDER_LINE_FIELDS)
        return order_line

    @abc.abstractmethod
    def _get_order_line(self, key):
        pass

    def store_order_line(self, key, order_line):
        assert set(order_line.keys()).issuperset(ORDER_LINE_FIELDS)
        return self._store_order_line(key, order_line)

    @abc.abstractmethod
    def _store_order_line(self, key, order_line):
        pass

    def get_item(self, key):
        item = self._get_item(key)
        assert set(item.keys()).issuperset(ITEM_FIELDS)
        return item

    @abc.abstractmethod
    def _get_item(self, key):
        pass

    def store_item(self, key, item):
        assert set(item.keys()).issuperset(ITEM_FIELDS)
        return self._store_item(key, item)

    @abc.abstractmethod
    def _store_item(self, key, item):
        pass

    def get_stock(self, key):
        stock = self._get_stock(key)
        assert set(stock.keys()).issuperset(STOCK_FIELDS)
        return stock

    @abc.abstractmethod
    def _get_stock(self, key):
        pass

    def store_stock(self, key, stock):
        assert set(stock.keys()).issuperset(STOCK_FIELDS)
        return self._store_stock(key, stock)

    @abc.abstractmethod
    def _store_stock(self, key, stock):
        pass

    def store_history(self, key, history):
        assert set(history.keys()).issuperset(HISTORY_FIELDS)
        return self._store_history(key, history)

    @abc.abstractmethod
    def _store_history(self, key, history):
        pass

class DatabaseAbort(Exception): pass

# Below this point is TPC-C's implementation.  No need to change anything to
# implement new backends.  Subclass the above Database object.

C = 0
CHARSET_A = string.ascii_letters + string.digits

def NURand(A, x, y):
    return (((random.randint(0, A) | random.randint(x, y)) + C) % (y - x + 1)) + x

def random_a_string(x, y):
    sz = random.randint(x, y)
    return ''.join([random.choice(CHARSET_A) for i in range(sz)])

def random_n_string(x, y):
    sz = random.randint(x, y)
    return ''.join([random.choice(string.digits) for i in range(sz)])

def zipcode():
    return random_n_string(4, 4) + '11111'

def lastname(idx):
    if idx >= 1000:
        idx = NURand(255, 0, 999)
    a = (idx // 100) % 10
    b = (idx // 10) % 10
    c = idx % 10
    strs = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']
    return strs[a] + strs[b] + strs[c]

WarehouseKey = collections.namedtuple('WarehouseKey', ('W_ID',))
DistrictKey = collections.namedtuple('DistrictKey', ('D_ID', 'W_ID'))
CustomerKey = collections.namedtuple('CustomerKey', ('C_ID', 'D_ID', 'W_ID'))
NewOrderKey = collections.namedtuple('NewOrderKey', ('O_ID', 'D_ID', 'W_ID'))
OrderKey = collections.namedtuple('OrderKey', ('O_ID', 'D_ID', 'W_ID'))
OrderLineKey = collections.namedtuple('OrderLineKey', ('O_ID', 'D_ID', 'W_ID', 'OL_NUMBER'))
ItemKey = collections.namedtuple('ItemKey', ('I_ID',))
StockKey = collections.namedtuple('StockKey', ('I_ID', 'W_ID'))
HistoryKey = collections.namedtuple('HistoryKey', ('C_ID', 'D_ID', 'W_ID'))

class Parameters(object):

    def __init__(self, W, D=10):
        self.WAREHOUSE = W
        self.DISTRICT  = D
        self.CUSTOMER  = 30000
        self.ORDER     = 30000
        self.ITEMS     = 100000
        self.STOCK     = 100000

    @property
    def CUSTOMER_PER_DISTRICT(self):
        return self.CUSTOMER // self.DISTRICT

    @property
    def NEW_ORDER_THRESHOLD(self):
        return self.CUSTOMER_PER_DISTRICT - 900 + 1

class PopulationGenerator(object):

    def __init__(self, db, params):
        self.db = db
        self.params = params

    def generate_item(self, item_id):
        return {'I_ID': item_id,
                'I_IM_ID': random.randint(1, 10000),
                'I_NAME': random_a_string(14, 24),
                'I_PRICE': random.randint(100, 10000),
                'I_DATA': random_a_string(26, 50)}

    def generate_stock(self, warehouse_id, stock_id):
        data = random_a_string(26, 50)
        if random.random() < 0.1:
            data = list(data)
            idx = random.randint(0, len(data) - 8)
            data [idx:idx + 8]= 'ORIGINAL'
            data = ''.join(data)
        return {'S_I_ID': stock_id,
                'S_W_ID': warehouse_id,
                'S_QUANTITY': random.randint(10, 100),
                'S_DIST_01': random_a_string(24, 24),
                'S_DIST_02': random_a_string(24, 24),
                'S_DIST_03': random_a_string(24, 24),
                'S_DIST_04': random_a_string(24, 24),
                'S_DIST_05': random_a_string(24, 24),
                'S_DIST_06': random_a_string(24, 24),
                'S_DIST_07': random_a_string(24, 24),
                'S_DIST_08': random_a_string(24, 24),
                'S_DIST_09': random_a_string(24, 24),
                'S_DIST_10': random_a_string(24, 24),
                'S_YTD': 0,
                'S_ORDER_CNT': 0,
                'S_REMOTE_CNT': 0,
                'S_DATA': data}

    def generate_warehouse(self, warehouse_id):
        return {'W_ID': warehouse_id,
                'W_NAME': random_a_string(6, 10),
                'W_STREET_1': random_a_string(10, 20),
                'W_STREET_2': random_a_string(10, 20),
                'W_CITY': random_a_string(10, 20),
                'W_STATE': random_a_string(2, 2),
                'W_ZIP': zipcode(),
                'W_TAX': random.uniform(0.0, 0.2),
                'W_YTD': 30000000}

    def generate_district(self, warehouse_id, district_id):
        return {'D_ID': district_id,
                'D_W_ID': warehouse_id,
                'D_NAME': random_a_string(6, 10),
                'D_STREET_1': random_a_string(10, 20),
                'D_STREET_2': random_a_string(10, 20),
                'D_CITY': random_a_string(10, 20),
                'D_STATE': random_a_string(2, 2),
                'D_ZIP': zipcode(),
                'D_TAX': random.uniform(0.0, 0.2),
                'D_YTD': 3000000,
                'D_NEXT_O_ID': self.params.CUSTOMER_PER_DISTRICT + 1}

    def generate_customer(self, warehouse_id, district_id, customer_id):
        return {'C_ID': customer_id,
                'C_D_ID': district_id,
                'C_W_ID': warehouse_id,
                'C_O_ID': self.params.CUSTOMER_PER_DISTRICT,
                'C_FIRST': random_a_string(8, 16),
                'C_MIDDLE': 'OE',
                'C_LAST': lastname(customer_id),
                'C_STREET_1': random_a_string(10, 20),
                'C_STREET_2': random_a_string(10, 20),
                'C_CITY': random_a_string(10, 20),
                'C_STATE': random_a_string(2, 2),
                'C_ZIP': zipcode(),
                'C_PHONE': random_n_string(16, 16),
                'C_SINCE': int(time.time() * 2**32),
                'C_CREDIT': 'BC' if random.random() < 0.1 else 'GC',
                'C_CREDIT_LIM': 5000000,
                'C_DISCOUNT': random.uniform(0, 0.5),
                'C_BALANCE': -1000,
                'C_YTD_PAYMENT': 1000,
                'C_PAYMENT_CNT': 1,
                'C_DELIVERY_CNT': 0,
                'C_DATA': random_a_string(300, 500)}

    def generate_history(self, warehouse_id, district_id, customer_id):
        return {'H_C_ID': customer_id,
                'H_C_D_ID': district_id,
                'H_C_W_ID': warehouse_id,
                'H_D_ID': district_id,
                'H_W_ID': warehouse_id,
                'H_DATE': int(time.time() * 2**32),
                'H_AMOUNT': 1000,
                'H_DATA': random_a_string(12, 24)}

    def generate_order(self, warehouse_id, district_id, order_id, customer_id):
        return {'O_ID': order_id,
                'O_D_ID': district_id,
                'O_W_ID': warehouse_id,
                'O_C_ID': customer_id,
                'O_ENTRY_D': int(time.time() * 2**32),
                'O_CARRIER_ID': random.randint(1, 10) if order_id < self.params.NEW_ORDER_THRESHOLD else 0,
                'O_OL_CNT': random.randint(5, 15),
                'O_ALL_LOCAL': 1}

    def generate_order_line(self, warehouse_id, district_id, order_id, order_line_id):
        return {'OL_O_ID': order_id,
                'OL_D_ID': district_id,
                'OL_W_ID': warehouse_id,
                'OL_NUMBER': order_line_id,
                'OL_I_ID': random.randint(1, self.params.ITEMS),
                'OL_SUPPLY_W_ID': warehouse_id,
                'OL_DELIVERY_D': int(time.time() * 2**32),
                'OL_QUANTITY': 5,
                'OL_AMOUNT': 0 if order_id < self.params.NEW_ORDER_THRESHOLD else random.randint(1, 999999),
                'OL_DIST_INFO': random_a_string(24, 24)}

    def generate_new_order(self, warehouse_id, district_id, order_id):
        return {'NO_O_ID': order_id,
                'NO_D_ID': district_id,
                'NO_W_ID': warehouse_id}

    def load_items(self):
        for i in range(1, self.params.ITEMS + 1):
            item = self.generate_item(i)
            item_key = ItemKey(I_ID=item['I_ID'])
            self.db.store_item(item_key, item)

    def load_warehouse(self, warehouse_id):
        '''load all rows that are unique to warehouse_id, but not predicated
        upon any one district within warehouse_id'''
        w = warehouse_id
        warehouse_key = WarehouseKey(W_ID=w)
        warehouse = self.generate_warehouse(w)
        self.db.store_warehouse(warehouse_key, warehouse)
        for s in range(1, self.params.STOCK + 1):
            stock = self.generate_stock(w, s)
            stock_key = StockKey(W_ID=stock['S_W_ID'], I_ID=stock['S_I_ID'])
            self.db.store_stock(stock_key, stock)

    def load_district(self, warehouse_id, district_id):
        '''load all rows that are unique to (warehouse_id, district_id)'''
        w = warehouse_id
        d = district_id
        district_key = DistrictKey(W_ID=w, D_ID=d)
        district = self.generate_district(w, d)
        self.db.store_district(district_key, district)
        for c in range(1, self.params.CUSTOMER_PER_DISTRICT + 1):
            customer_key = CustomerKey(W_ID=w, D_ID=d, C_ID=c)
            customer = self.generate_customer(w, d, c)
            self.db.store_customer(customer_key, customer)
            history_key = HistoryKey(W_ID=w, D_ID=d, C_ID=c)
            history = self.generate_history(w, d, c)
            self.db.store_history(history_key, history)
        customer_permutation = list(range(1, self.params.CUSTOMER_PER_DISTRICT + 1))
        random.shuffle(customer_permutation)
        for o in range(1, self.params.CUSTOMER_PER_DISTRICT + 1):
            c = customer_permutation[o - 1]
            order_key = OrderKey(W_ID=w, D_ID=d, O_ID=o)
            order = self.generate_order(w, d, o, c)
            self.db.store_order(order_key, order)
            for ol in range(1, order['O_OL_CNT'] + 1):
                order_line_key = OrderLineKey(W_ID=w, D_ID=d, O_ID=o, OL_NUMBER=ol)
                order_line = self.generate_order_line(w, d, o, ol)
                self.db.store_order_line(order_line_key, order_line)
            if o >= self.params.NEW_ORDER_THRESHOLD:
                new_order_key = NewOrderKey(W_ID=w, D_ID=d, O_ID=o)
                new_order = self.generate_new_order(w, d, o)
                self.db.store_new_order(new_order_key, new_order)

    def load_all(self):
        self.load_items()
        for w in range(self.params.WAREHOUSE):
            w += 1
            self.load_warehouse(w)
            for d in range(self.params.DISTRICT):
                d += 1
                self.load_district(w, d)

class TransactionGenerator(object):

    def __init__(self, db, params, num_ops, new_order_only):
        self.db = db
        self.params = params
        self.num_ops = num_ops
        self.new_order_only = new_order_only

    def generate_W_ID(self):
        return random.randint(1, self.params.WAREHOUSE)

    def generate_D_ID(self):
        return random.randint(1, self.params.DISTRICT)

    def new_order_transaction(self, W_ID, D_ID):
        C_ID = NURand(1023, 1, self.params.CUSTOMER_PER_DISTRICT)
        self.db.begin_transaction()
        warehouse_key = WarehouseKey(W_ID=W_ID)
        warehouse = self.db.get_warehouse(warehouse_key)
        district_key = DistrictKey(W_ID=W_ID, D_ID=D_ID)
        district = self.db.get_district(district_key)
        order_id = district['D_NEXT_O_ID']
        district['D_NEXT_O_ID'] += 1
        self.db.store_district(district_key, district)
        customer_key = CustomerKey(W_ID=W_ID, D_ID=D_ID, C_ID=C_ID)
        customer = self.db.get_customer(customer_key)
        customer['C_O_ID'] = order_id
        self.db.store_customer(customer_key, customer)
        new_order_key = NewOrderKey(W_ID=W_ID, D_ID=D_ID, O_ID=order_id)
        new_order = {'NO_O_ID': order_id,
                     'NO_D_ID': D_ID,
                     'NO_W_ID': W_ID}
        self.db.store_new_order(new_order_key, new_order)
        ordered_items = random.randint(1, 10)
        order = {'O_ID': order_id,
                 'O_D_ID': D_ID,
                 'O_W_ID': W_ID,
                 'O_C_ID': C_ID,
                 'O_ENTRY_D': int(time.time() * 2**32),
                 'O_CARRIER_ID': 0,
                 'O_OL_CNT': ordered_items,
                 'O_ALL_LOCAL': 1}
        order_lines = []
        for i in range(ordered_items):
            order_line = {'OL_I_ID': NURand(8191, 1, self.params.ITEMS),
                          'OL_SUPPLY_W_ID': W_ID,
                          'OL_DELIVERY_D': 0,
                          'OL_D_ID': D_ID,
                          'OL_W_ID': W_ID,
                          'OL_QUANTITY': random.randint(1, 10)}
            if random.randint(1, 100) == 1:
                order['O_ALL_LOCAL'] = 0
                while order_line['OL_SUPPLY_W_ID'] == W_ID and self.params.WAREHOUSE > 1:
                    order_line['OL_SUPPLY_W_ID'] = self.generate_W_ID()
            order_lines.append(order_line)
        # this is the 1% random rollback
        rollback_case = random.randint(1, 100) == 1
        order_key = OrderKey(W_ID=W_ID, D_ID=D_ID, O_ID=order_id)
        self.db.store_order(order_key, order)
        for i, order_line in enumerate(order_lines):
            item = self.db.get_item(ItemKey(order_line['OL_I_ID']))
            stock_key = StockKey(I_ID=item['I_ID'], W_ID=W_ID)
            stock = self.db.get_stock(stock_key)
            if stock['S_QUANTITY'] >= order_line['OL_QUANTITY'] + 10:
                stock['S_QUANTITY'] = stock['S_QUANTITY'] - order_line['OL_QUANTITY']
            else:
                stock['S_QUANTITY'] = (stock['S_QUANTITY'] - order_line['OL_QUANTITY']) + 91
            stock['S_YTD'] = stock['S_YTD'] + order_line['OL_QUANTITY']
            stock['S_ORDER_CNT'] = stock['S_ORDER_CNT'] + 1
            if order_line['OL_SUPPLY_W_ID'] != W_ID:
                stock['S_REMOTE_CNT'] = stock['S_REMOTE_CNT'] + 1
            self.db.store_stock(stock_key, stock)
            order_line['OL_AMOUNT'] = order_line['OL_QUANTITY'] * item['I_PRICE']
            if 'ORIGINAL' in item['I_DATA'] and 'ORIGINAL' in item['S_DATA']:
                brand_generic = 'B'
            else:
                brand_generic = 'G'
            order_line['OL_NUMBER'] = i + 1
            order_line['OL_O_ID'] = order_id
            order_line['OL_DIST_INFO'] = stock['S_DIST_%02d' % (((D_ID - 1) % 10) + 1)] # XXX
            order_line_key = OrderLineKey(W_ID=W_ID, D_ID=D_ID, O_ID=order_id,
                    OL_NUMBER = i + 1)
            self.db.store_order_line(order_line_key, order_line)
        if rollback_case:
            self.db.abort_transaction()
        else:
            self.db.commit_transaction()

    def payment_transaction(self, W_ID, D_ID):
        C_ID = NURand(1023, 1, self.params.CUSTOMER_PER_DISTRICT)
        C_W_ID = W_ID
        C_D_ID = D_ID
        if random.randint(1, 100) >= 85:
            C_D_ID = self.generate_D_ID()
            while C_W_ID == W_ID and self.params.WAREHOUSE > 1:
                C_W_ID = self.generate_W_ID()
        pay_amount = random.randint(100, 500000)
        history = {'H_AMOUNT': pay_amount,
                   'H_DATE': int(time.time() * 2**32),
                   'H_C_D_ID': C_D_ID,
                   'H_C_W_ID': C_W_ID,
                   'H_D_ID': D_ID,
                   'H_W_ID': W_ID,
                   'H_C_ID': C_ID}
        self.db.begin_transaction()
        warehouse_key = WarehouseKey(W_ID=W_ID)
        warehouse_fields = ('W_NAME', 'W_STREET_1', 'W_STREET_2', 'W_CITY', 'W_STATE', 'W_ZIP')
        if not self.db.ATOMIC:
            warehouse_fields += ('W_YTD',)
        warehouse = self.db.get_warehouse(warehouse_key)
        if self.db.ATOMIC:
            self.db.bump_warehouse_payment(warehouse_key, pay_amount)
        else:
            warehouse['W_YTD'] += pay_amount
            self.db.store_warehouse(warehouse_key, warehouse)
        district_key = DistrictKey(W_ID=W_ID, D_ID=D_ID)
        district_fields = ('D_NAME', 'D_STREET_1', 'D_STREET_2', 'D_CITY', 'D_STATE', 'D_ZIP')
        if not self.db.ATOMIC:
            district_fields += ('D_YTD',)
        district = self.db.get_district(district_key)
        if self.db.ATOMIC:
            self.db.bump_district_payment(district_key, pay_amount)
        else:
            district['D_YTD'] += pay_amount
            self.db.store_district(district_key, district)
        customer_key = CustomerKey(W_ID=C_W_ID, D_ID=C_D_ID, C_ID=C_ID)
        customer = self.db.get_customer(customer_key)
        customer.update({'C_BALANCE': customer['C_BALANCE'] - pay_amount,
                         'C_YTD_PAYMENT': customer['C_YTD_PAYMENT'] + pay_amount,
                         'C_PAYMENT_CNT': customer['C_PAYMENT_CNT'] + 1})
        if customer['C_CREDIT'] == 'BC':
            customer['C_DATA'] = str((C_ID, C_D_ID, C_W_ID, D_ID, W_ID, pay_amount)) + customer['C_DATA']
            customer['C_DATA'] = customer['C_DATA'][:500]
        self.db.store_customer(customer_key, customer)
        history['H_DATA'] = warehouse['W_NAME'] + ' '*4 + district['D_NAME']
        history_key = HistoryKey(W_ID=C_W_ID, D_ID=C_D_ID, C_ID=C_ID)
        self.db.store_history(history_key, history)
        self.db.commit_transaction()

    def order_status_transaction(self, W_ID, D_ID):
        C_ID = NURand(1023, 1, self.params.CUSTOMER_PER_DISTRICT)
        self.db.begin_transaction()
        customer_key = CustomerKey(W_ID=W_ID, D_ID=D_ID, C_ID=C_ID)
        customer = self.db.get_customer(customer_key)
        order_key = OrderKey(W_ID=W_ID, D_ID=D_ID, O_ID=customer['C_O_ID'])
        order = self.db.get_order(order_key)
        for i in range(1, order['O_OL_CNT'] + 1):
            order_line_key = OrderLineKey(W_ID=W_ID, D_ID=D_ID,
                                          O_ID=customer['C_O_ID'],
                                          OL_NUMBER=i)
            order_line = self.db.get_order_line(order_line_key)
        self.db.commit_transaction()

    def stock_level_transaction(self, W_ID, D_ID):
        thresh = random.randint(10, 20)
        self.db.begin_transaction()
        district_key = DistrictKey(W_ID=W_ID, D_ID=D_ID)
        district = self.db.get_district(district_key)
        stocks = set()
        for i in range(max(0, district['D_NEXT_O_ID'] - 20), district['D_NEXT_O_ID']):
            order_key = OrderKey(W_ID=W_ID, D_ID=D_ID, O_ID=i)
            order = self.db.get_order(order_key)
            if order is None:
                continue
            for j in range(1, order['O_OL_CNT'] + 1):
                order_line_key = OrderLineKey(W_ID=W_ID, D_ID=D_ID, O_ID=i, OL_NUMBER=j)
                order_line = self.db.get_order_line(order_line_key)
                if order_line is None:
                    continue # the 1% aborted cause this
                stocks.add(order_line['OL_I_ID'])
        count = 0
        for s in set(stocks):
            stock_key = StockKey(I_ID=s, W_ID=W_ID)
            stock = self.db.get_stock(stock_key)
            if stock and stock['S_QUANTITY'] < thresh:
                count += 1
        self.db.commit_transaction()

    def generate_ops(self):
        deck = []
        deck += [('new-order', self.new_order_transaction)] * 10
        if not self.new_order_only:
            deck += [('payment', self.payment_transaction)] * 10
            deck += [('order-status', self.order_status_transaction)]
            deck += [('stock-level', self.stock_level_transaction)]
        def infinite_deck():
            while True:
                random.shuffle(deck)
                for x in deck:
                    yield x
        return itertools.islice(infinite_deck(), self.num_ops)

    def run_transactions(self, W_ID, D_ID, dl):
        for series, card in self.generate_ops():
            w = W_ID if W_ID is not None else self.generate_W_ID()
            d = D_ID if D_ID is not None else self.generate_D_ID()
            aborts = 0
            start = time.time()
            while True:
                try:
                    card(w, d)
                    end = time.time()
                    dl.record(series, int(end * 1000), (end - start) * 1000)
                    break
                except DatabaseAbort as e:
                    aborts += 1
            # XXX record aborts

def main_dummy(args, db):
    print("did nothing; probably a bug", file=sys.stderr)
    return -1

def main_setup(args, db):
    db.setup()
    return 0

def main_wipe(args, db):
    db.wipe()
    return 0

def main_load_items(args, db):
    params = Parameters(args.warehouses, args.districts)
    pg = PopulationGenerator(db, params)
    pg.load_items()
    return 0

def generate_p(limit, arg):
    if arg is None:
        for w in range(limit):
            yield w + 1
    else:
        assert 1 <= arg <= limit
        yield arg

def main_load_warehouse(args, db):
    params = Parameters(args.warehouses, args.districts)
    pg = PopulationGenerator(db, params)
    for w in generate_p(params.WAREHOUSE, args.warehouse):
        pg.load_warehouse(w)
    return 0

def main_load_district(args, db):
    params = Parameters(args.warehouses, args.districts)
    pg = PopulationGenerator(db, params)
    for w in generate_p(params.WAREHOUSE, args.warehouse):
        for d in generate_p(params.DISTRICT, args.district):
            pg.load_district(w, d)
    return 0

def main_load_all(args, db):
    params = Parameters(args.warehouses, args.districts)
    pg = PopulationGenerator(db, params)
    pg.load_all()
    return 0

def main_run(args, db):
    params = Parameters(args.warehouses, args.districts)
    tg = TransactionGenerator(db, params, args.operations, new_order_only=args.new_order_only)
    dl = ygor.collect.DataLogger(args.output,
            [ygor.collect.Series(name='new-order', indep_units='ms', indep_precision='precise', dep_units='ms', dep_precision='half'),
             ygor.collect.Series(name='payment', indep_units='ms', indep_precision='precise', dep_units='ms', dep_precision='half'),
             ygor.collect.Series(name='order-status', indep_units='ms', indep_precision='precise', dep_units='ms', dep_precision='half'),
             ygor.collect.Series(name='stock-level', indep_units='ms', indep_precision='precise', dep_units='ms', dep_precision='half')])
    try:
        tg.run_transactions(args.warehouse, args.district, dl)
    finally:
        dl.flush_and_destroy()

def main(argv):
    if len(argv) < 2:
        print("usage: <action> <binding>", file=sys.stderr)
        return -1
    action = argv[0]
    binding = argv[1]
    if '.' not in binding:
        binding = ('tpcc_kv.db_' + binding).strip('.')
    argv = argv[2:]
    random.seed(os.urandom(8))

    # Figure out the action to perform
    parser = argparse.ArgumentParser()
    load_common = False
    nested_main = main_dummy
    if action == 'setup':
        load_common = False
        nested_main = main_setup
    elif action == 'wipe':
        load_common = False
        nested_main = main_wipe
    elif action == 'load-items':
        load_common = True
        nested_main = main_load_items
    elif action == 'load-warehouse':
        parser.add_argument('--warehouse', type=int, default=None)
        load_common = True
        nested_main = main_load_warehouse
    elif action == 'load-district':
        parser.add_argument('--warehouse', type=int, default=None)
        parser.add_argument('--district', type=int, default=None)
        load_common = True
        nested_main = main_load_district
    elif action == 'load-all':
        load_common = True
        nested_main = main_load_all
    elif action == 'run':
        load_common = False
        nested_main = main_run
        parser.add_argument('--operations', type=int, default=1000)
        parser.add_argument('--new-order-only', action='store_true', default=False)
        parser.add_argument('--warehouses', type=int, default=10)
        parser.add_argument('--warehouse', type=int, default=None)
        parser.add_argument('--districts', type=int, default=10)
        parser.add_argument('--district', type=int, default=None)
        parser.add_argument('--output', type=str, default='tpcc-kv.out')
    else:
        print("don't know how to %r" % action, file=sys.stderr)
        return -1
    # Common for "load-*" actions
    if load_common:
        parser.add_argument('--warehouses', type=int, default=10)
        parser.add_argument('--districts', type=int, default=10)

    # Figure out the database to use
    try:
        db_mod = importlib.import_module(binding)
    except ImportError as e:
        print(e, file=sys.stderr)
        print("couldn't import binding from %r" % binding, file=sys.stderr)
        print('check if you can import the module manually', file=sys.stderr)
        return -1
    if not hasattr(db_mod, 'add_arguments') or not hasattr(db_mod, 'create_database'):
        print("couldn't create database from %r" % binding, file=sys.stderr)
        print('complain to the person who wrote that module', file=sys.stderr)
        return -1

    # Parse the arguments and create the db
    db_mod.add_arguments(parser)
    args = parser.parse_args(argv)
    db = db_mod.create_database(args)
    return nested_main(args, db)

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]) or 0)
