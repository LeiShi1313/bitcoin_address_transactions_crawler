# -*- coding: utf-8 -*-
import scrapy
import MySQLdb
from myconfig import MySQLConfig
from bs4 import BeautifulSoup as bs4
from wallet_address_crawler.items import AddressItem

class WalletAddressSpider(scrapy.Spider):
    name = "wallet_address"
    allowed_domains = ["walletexplorer.com"]
    url = 'http://walletexplorer.com'
    headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, sdch, br',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4,ja;q=0.2',
            'Host': 'www.walletexplorer.com',
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
                    "select url, wallet_id from wallet_urls")
            results = self.cursor.fetchall()
        except MySQLdb.Error, e:
            print 'Error {} {}'.format(e.args[0], e.args[1])

        self.d = {}
        self.start_urls = []
        for url, wallet_id in results:
            self.d[url] = wallet_id
            self.start_urls.append(url+"/addresses")

    def start_requests(self):
        requests = []
        for url in self.start_urls:
            requests.append(scrapy.Request(
                url = url,
                headers = self.headers,
                callback = self.parse_wallet))
        return requests

    def parse_wallet(self, response):
        soup = bs4(response.body, 'lxml')
        pages = int(soup.find('div', class_='paging').text.split(' / ')[1].split(' ')[0])
        for i in xrange(pages):
            yield scrapy.Request(
                    url = response.url + '?page={}'.format(i+1),
                    headers = self.headers,
                    callback = self.parse_one_page)

    def parse_one_page(self, response):
        soup = bs4(response.body, 'lxml')
        for tr in soup.table.find_all('tr'):
            if tr.td and tr.td.a:
                address_item = AddressItem()
                address_item['address'] = tr.td.a.text
                address_item['wallet_id'] = self.d[response.url.split('/addresses')[0]]
                yield address_item
