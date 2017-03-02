# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
import logging
import pymongo
from myconfig import MySQLConfig
from myconfig import MongoConfig
from wallet_address_crawler.items import AddressItem
from wallet_address_crawler.items import AddrTxItem

class WalletAddressCrawlerPipeline(object):
    def __init__(self):
        self.conn = MySQLdb.connect(
                user = MySQLConfig['user'],
                passwd = MySQLConfig['passwd'],
                db = MySQLConfig['db'],
                host = MySQLConfig['host'],
                charset = 'utf8',
                use_unicode = True)
        self.cursor = self.conn.cursor()
    def process_item(self, item, spider):
        if not isinstance(item, AddressItem):
            return item
        try:
            self.cursor.execute(
                    """insert into addresses(addr, wallet_id) values
                    ("{}",{})""".format(item['address'], item['wallet_id']))
            self.conn.commit()
        except MySQLdb.Error, e:
            print 'Error {} {}'.format(e.args[0], e.args[1])

        return item


class AddressTransactionPipeline(object):
    def __init__(self):
        self.mongo_uri = MongoConfig['host']
        self.mongo_port = MongoConfig['port']
        self.mongo_db = MongoConfig['database']
        self.mongo_col = MongoConfig['collection']

    def open_spider(self, spider):
        try:
            self.client = pymongo.MongoClient(self.mongo_uri, self.mongo_port)
            self.db = self.client[self.mongo_db][self.mongo_col]
        except pymongo.errors, e:
            logging.error(e)

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if not isinstance(item, AddrTxItem):
            return item

        self.db.insert(dict(item))

