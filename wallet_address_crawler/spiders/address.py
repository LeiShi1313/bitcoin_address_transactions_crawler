# -*- coding: utf-8 -*-
import scrapy
import MySQLdb
import json
import logging
import pymongo
from myconfig import MySQLConfig
from myconfig import MongoConfig
from bs4 import BeautifulSoup as bs4
from wallet_address_crawler.items import AddressItem
from wallet_address_crawler.items import AddrTxItem

class WalletAddressSpider(scrapy.Spider):
    name = "address"
    allowed_domains = ["blockchain.info"]
    url_template = 'https://blockchain.info/address/{}?format=json'
    headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, sdch, br',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4,ja;q=0.2',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'
            }

    def __init__(self):
        self.conn = MySQLdb.connect(
                user = MySQLConfig['user'],
                passwd = MySQLConfig['passwd'],
                db = MySQLConfig['db'],
                host = MySQLConfig['host'],
                charset = 'utf8',
                use_unicode = True)
        self.cursor = self.conn.cursor()
        try:
            self.cursor.execute(
                    """select count(*) from addresses""")
            results = self.cursor.fetchall()
            self.max = int(results[0][0])
            print "Max Address: {}".format(self.max)
        except MySQLdb.Error, e:
            print 'Error {} {}'.format(e.args[0], e.args[1])
        self.start_urls = ["http://baidu.com"]
        self.client = pymongo.MongoClient(MongoConfig['host'], MongoConfig['port'])
        self.db = self.client[MongoConfig['database']][MongoConfig['collection']]
        logging.info("init ended")

    def start_requests(self):
        self.d = {}
        limit = 5000
        offset = 1000000
        logging.info("Loop init")
        while offset<self.max:
            print "Running at offset: {}".format(offset)
            try:
                self.cursor.execute(
                        """select addr, wallet_name, service_name, type_name, address_id
                        from addresses natural join wallets
                        natural join services natural join types
                        where address_id>={} and address_id<{} 
                        and address_id%100 = 0""".format(offset, offset+limit))
                results = self.cursor.fetchall()
                offset += limit
                if len(results) == 0:
                    continue
                for addr, wallet, service, tp, index in results:
                    if self.db.find_one({'_id': addr}):
                        logging.info("{} {} skiped!".format(addr, index))
                        continue
                    self.d[addr] = {
                            "wallet": wallet,
                            "service": service,
                            "type": tp,
                            "index": index
                            }
                    url = self.url_template.format(addr)
                    yield scrapy.Request(
                        url = url,
                        headers = self.headers,
                        callback = self.parse_address)
            except MySQLdb.Error, e:
                print 'Error {} {}'.format(e.args[0], e.args[1])


    def parse_address(self, response):
        addr = json.loads(response.body)
        address = addr['address']
        num_txs = addr['n_tx']
        address_tx_item = AddrTxItem()
        if num_txs > len(addr['txs']):
            address_tx_item['more'] = 1
        else:
            address_tx_item['more'] = 0
        address_tx_item['n_tx'] = num_txs
        address_tx_item["total_received"] = addr["total_received"]
        address_tx_item["total_sent"] = addr["total_sent"]
        address_tx_item["_id"] = addr['address']
        address_tx_item["txs"] = addr['txs']
        try:
            address_tx_item['wallet'] = self.d[address]['wallet']
            address_tx_item['service'] = self.d[address]['service']
            address_tx_item['t'] = self.d[address]['type']
            address_tx_item['index'] = self.d[address]['index']
            del self.d[address]
        except KeyError, e:
            logging.error("Address: {} KEYRROR!!!!!!!!!!".format(address))
            logging.error(e)
            address_tx_item['wallet'] = "Unknown"
            address_tx_item['service'] = "Unknown"
            address_tx_item['t'] = "Unknown"
            address_tx_item['index'] = "Unknown"

        logging.info("Storing addr {} index {}".format(address, address_tx_item['index']))
        yield address_tx_item


