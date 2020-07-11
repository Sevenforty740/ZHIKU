# -*- coding: utf-8 -*-
import scrapy
import socket
import re
import langid
import requests
import time
from datetime import datetime
from ZHIKU.items import ReportItem, NewsItem, PolicyItem
from RAKE.rake import Rake
from jieba.analyse import extract_tags
from lxml import etree
import json
import uuid
from urllib.parse import unquote
import os

rake = Rake(
    r"D:\ProgramData\Anaconda3\Lib\RAKE\SmartStoplist.txt")
# rake = Rake(
#     r"/usr/local/python3/lib/python3.7/site-packages/RAKE/SmartStoplist.txt")


def get_host_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def turn_italino_date(date):
    month_intalino = {
        'gennaio': '1',
        'febbraio': '2',
        'marzo': '3',
        'aprile': '4',
        'maggio': '5',
        'giugno': '6',
        'luglio': '7',
        'agosto': '8',
        'settembre': '9',
        'ottobre': '10',
        'novembre': '11',
        'dicembre': '12'
    }
    for month, month_n in month_intalino.items():
        if month in date.lower():
            date = date.lower().replace(month, month_n)
    return date


def test(url):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cookie': 'IEA.WEBSTORE.AUTH=CBBA6190011CE56D7A64357A5FE943C4DAF38AE083429CB4BA8E45C9DD3905D835102AD9A17B2C96667B263189D425F1B682CB856C8B168963FB67C62E3F5D249EEF800281CBBAF2BE46D29FB8B83E2C057DB41E2BC6EE1B64BD55E0D1357952AB551320464AFB876D9408814B0977346900AEAEBD7DE2314A0B38F33A3B1BC04AF6C18E8453FCB3D289370C66B9903C93C37609D31DDF1FFCD79B0C402B7616;',
        'Host': 'webstore.iea.org',
        'Referer': 'https://webstore.iea.org/',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
    }

    res = requests.head(url, headers=headers, allow_redirects=False)
    if 'Location' in res.headers:
        return res.headers['Location']
    else:
        return url


class IEASpider(scrapy.Spider):
    name = 'iea'
    allowed_domains = ['iea.org']
    start_urls = ['https://webstore.iea.org/']

    def parse(self, response):
        categorys = response.xpath(
            "//div[@class='category-item']/div[@class='picture']/a/@href").extract()
        for categorylink in categorys:
            cate_url = 'https://webstore.iea.org' + categorylink
            yield scrapy.Request(cate_url, callback=self.category_parser)

    def category_parser(self, response):
        boxes = response.xpath("//div[@class='product-item']")
        for box in boxes:
            item = ReportItem()
            item['title'] = box.xpath(
                ".//div[@class='details']/h2[@class='product-title']/a/text()").extract_first()
            item['url'] = 'https://webstore.iea.org/' + box.xpath(
                ".//div[@class='details']/h2[@class='product-title']/a/@href").extract_first()
            type = box.xpath(
                ".//div[@class='details']/div[@class='add-info']/div[@class='buttons']/button/text()").extract_first()
            if type.lower() == 'download':
                yield scrapy.Request(item['url'], callback=self.download, meta={'item': item.copy(), 'can_download': 1})
            else:
                yield scrapy.Request(item['url'], callback=self.download, meta={'item': item.copy(), 'can_download': 0})
        # 翻页
        next = response.xpath(
            "//li[@class='next-page']/a/@href").extract_first()
        if next:
            next = "https://webstore.iea.org" + next
            yield scrapy.Request(next, callback=self.category_parser)

    def download(self, response):
        can_download = response.meta.get('can_download')
        item = response.meta.get('item')
        content = response.xpath("//div[@id='full-description']/p")
        content = content.xpath("string(.)").extract()
        content = "\n".join(content)
        item['crawler_ip'] = get_host_ip()
        item['category'] = 'report'
        item['target'] = 'webstore.iea.org'
        item['organization'] = '国际能源署'
        item['content'] = content
        item['abstract'] = content
        item['language'] = langid.classify(content)[0]
        keyword = response.xpath(
            "//div[@class='product-tags-list']/ul/li/a/text()").extract()
        if keyword:
            item['keyword'] = ";".join([word.strip() for word in keyword])
        publish_time = response.xpath(
            "//td[contains(text(),'Date')]/following-sibling::td/text()").extract_first()
        if publish_time:
            try:
                item['publish_time'] = datetime.strptime(
                    publish_time.strip(), "%d %B %Y")
                item['year'] = datetime.strftime(item['publish_time'], "%Y")
            except BaseException:
                item['publish_time'] = None

        item['init_time'] = datetime.now()
        pages = response.xpath(
            "//td[contains(text(),'Pages')]/following-sibling::td/text()").extract_first()
        if pages:
            try:
                item['pages'] = int(pages.strip())
            except BaseException:
                item['pages'] = None
        if can_download:
            id = response.xpath(
                "//div[@class='product-variant-line']/@data-productid").extract_first()
            download_url = "https://webstore.iea.org/download/direct/" + id
            result = test(download_url)
            item['file_urls'] = [result]
        item['uuid'] = str(uuid.uuid1())
        yield item


class CDFSpider(scrapy.Spider):
    name = 'cdf'
    allowed_domains = ['cdf-salvettifoundation.it']
    start_urls = ['https://www.cdf-salvettifoundation.it/reports-2/']

    def parse(self, response):
        boxes = response.xpath(
            "//div[contains(@class,'x-section')][2]/div[contains(@class,'x-container')]")
        for box in boxes:
            item = ReportItem()
            english = box.xpath(
                ".//div[contains(@class,'x-column')][last()]/div/p")
            if english:
                r = box.xpath(
                    "string(.//div[contains(@class,'x-column')][last()]/div[contains(@class,'x-text')])").extract_first()
                l = r.split("\n")
                for i in range(l.count("")):
                    l.remove("")
                publish_time = l[0]
                if publish_time:
                    publish_time = publish_time.strip()
                title = l[1].strip()
                author, organization, file_url, abstract = None, None, None, ""
                for i in l[2:]:
                    if i.startswith("Aut:"):
                        author = i[4:].strip()
                    elif i.startswith("Pub:"):
                        organization = i[4:].strip()
                    elif i.startswith("http") or i.startswith("www"):
                        file_url = i
                        if l.index(i) != len(l) - 1:
                            file_url = file_url + l[-1]
                    else:
                        if l.index(i) == len(l) - 1:
                            continue
                        abstract += i
                item['crawler_ip'] = get_host_ip()
                item['uuid'] = str(uuid.uuid1())
                item['category'] = 'report'
                item['target'] = 'www.cdf-salvettifoundation.it'
                item['language'] = 'en'
                try:
                    item['publish_time'] = datetime.strptime(
                        publish_time, "%B %Y")
                except BaseException:
                    publish_time = re.sub(r"[a-z]+,", "", publish_time)
                    item['publish_time'] = datetime.strptime(
                        publish_time, "%B %d %Y")
                try:
                    item['year'] = datetime.strftime(
                        item['publish_time'], "%Y")
                except BaseException:
                    pass
                item['init_time'] = datetime.now()
                item['url'] = 'https://www.cdf-salvettifoundation.it/reports-2/'
                item['title'] = title
                item['author'] = author
                item['organization'] = '萨尔维蒂基金会'
                item['abstract'] = abstract
                item['content'] = abstract
                keywords = rake.run(abstract)
                keywords = [tuple[0] for tuple in keywords[:3]]
                item['keyword'] = ';'.join(keywords)
                item['file_urls'] = [file_url]
            else:
                r = box.xpath(
                    "string(.//div[contains(@class,'x-column')][2]/div[contains(@class,'x-text')])").extract_first()
                l = r.split("\n")
                for i in range(l.count("")):
                    l.remove("")
                publish_time = l[0]
                if publish_time:
                    publish_time = turn_italino_date(publish_time.strip())
                title = l[1].strip()
                author, organization, file_url, abstract = None, None, None, ""
                for i in l[2:]:
                    if i.startswith("Aut:"):
                        author = i[4:].strip()
                    elif i.startswith("Pub:"):
                        organization = i[4:].strip()
                    elif i.startswith("http") or i.startswith("www"):
                        file_url = i
                        if l.index(i) != len(l) - 1:
                            file_url = file_url + l[-1]
                    else:
                        if l.index(i) == len(l) - 1:
                            continue
                        abstract += i
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'report'
                item['target'] = 'CENTRO DOCUMENTAZIONE FORMAZIONE FONDAZIONE SALVETTI'
                item['language'] = 'it'
                try:
                    item['publish_time'] = datetime.strptime(
                        publish_time, "%m %Y")
                except BaseException:
                    item['publish_time'] = datetime.strptime(
                        publish_time, "%d %m %Y")
                item['init_time'] = datetime.now()
                item['url'] = 'https://www.cdf-salvettifoundation.it/reports-2/'
                item['title'] = title
                item['author'] = author
                item['organization'] = organization
                item['abstract'] = abstract
                item['content'] = abstract
                item['file_urls'] = [file_url]
            yield item


class UnenvironmentSpider(scrapy.Spider):
    name = 'unenvironment'
    allowed_domains = ['unenvironment.org']

    def start_requests(self):
        start_dict = {
            # 'https://www.unenvironment.org/resources?f%5B0%5D=type%3A48&keywords=&page=0':'news',
            'https://www.unenvironment.org/resources?keywords=&f%5B0%5D=type%3A53': 'report'
        }
        for url, category in start_dict.items():
            if category == 'report':
                yield scrapy.Request(url, callback=self.report_parser)
            else:
                yield scrapy.Request(url, callback=self.news_parser)

    def report_parser(self, response):
        boxes = response.xpath("//div[@class='result_item']")
        for box in boxes:
            type = box.xpath(".//span[@class='type']/text()").extract_first()
            if 'report' in type.strip().lower():
                item = ReportItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'report'
                item['target'] = 'www.unenvironment.org'
                item['organization'] = '联合国环境署'
                item['language'] = 'en'
                publish_time = response.xpath(
                    ".//span[@class='date']/text()").extract_first()
                if publish_time:
                    item['publish_time'] = datetime.strptime(
                        publish_time.strip(), "%d %b %Y")
                    item['year'] = datetime.strftime(
                        item['publish_time'], "%Y")
                url = box.xpath(
                    ".//div[@class='result_item_title']/h5/a/@href").extract_first()
                item['url'] = 'https://www.unenvironment.org' + url
                yield scrapy.Request(item['url'], callback=self.report_detail_parser, meta={'item': item.copy()})

        next_page = response.xpath("//a[@rel='next']/@href").extract_first()
        if next_page:
            next_page = "https://www.unenvironment.org/resources" + next_page
            yield scrapy.Request(next_page, callback=self.report_parser)

    def report_detail_parser(self, response):
        item = response.meta.get('item')
        item['title'] = response.xpath(
            "//div[@class='report_header_title']/h1/text()").extract_first()
        item['abstract'] = response.xpath(
            "string(//article[@id='ThisOne']/div/div)").extract_first()
        item['init_time'] = datetime.now()
        item['author'] = response.xpath(
            "//div[@class='report_header_author']/text()").extract_first()
        item['author'] = item['author'].strip() if item['author'] else None
        content = response.xpath("string(//article)").extract_first()
        item['abstract'] = content.strip() if content else None
        item['content'] = item['abstract']
        item['uuid'] = str(uuid.uuid1())
        keyword = response.xpath(
            "//div[@class='document_topics']//li//text()").extract()
        if keyword:
            item['keyword'] = ";".join(keyword)
        links_name = response.xpath(
            "//div[@class='content_sidebar']//a/text()").extract()
        links = response.xpath(
            "//div[@class='content_sidebar']//a/@href").extract()
        if len(links) > 1:
            for name, link in zip(links_name, links):
                if 'full' in name.lower():
                    try:
                        link = re.findall(r"(.*\..*)\?.*", link)[0]
                    except BaseException:
                        pass
                    if link.startswith('https://wedocs.unep.org/handle/'):
                        yield scrapy.Request(link, callback=self.wedocdownload, meta={'item': item.copy()})
                        return
                    item['file_urls'] = [link]
                    break
                elif 'chinese' in name.lower():
                    try:
                        link = re.findall(r"(.*\..*)\?.*", link)[0]
                    except BaseException:
                        pass
                    if link.startswith('https://wedocs.unep.org/handle/'):
                        yield scrapy.Request(link, callback=self.wedocdownload, meta={'item': item.copy()})
                        return
                    item['file_urls'] = [link]
                    break
                elif 'english' in name.lower():
                    try:
                        link = re.findall(r"(.*\..*)\?.*", link)[0]
                    except BaseException:
                        pass
                    if link.startswith('https://wedocs.unep.org/handle/'):
                        yield scrapy.Request(link, callback=self.wedocdownload, meta={'item': item.copy()})
                        return
                    item['file_urls'] = [link]
                    break
            else:
                try:
                    link = re.findall(r"(.*\..*)\?.*", links[0])[0]
                except BaseException:
                    link = links[0]
                if link.startswith('https://wedocs.unep.org/handle/'):
                    yield scrapy.Request(link, callback=self.wedocdownload, meta={'item': item.copy()})
                    return
                item['file_urls'] = [link]

        elif len(links) == 1:
            try:
                link = re.findall(r"(.*\..*)\?.*", links[0])[0]
            except BaseException:
                link = links[0]
            if link.startswith('https://wedocs.unep.org/handle/'):
                yield scrapy.Request(link, callback=self.wedocdownload, meta={'item': item.copy()})
                return
            item['file_urls'] = [link]
        yield item

    def news_parser(self, response):
        boxes = response.xpath("//div[@class='result_item']")
        for box in boxes:
            type = box.xpath(".//span[@class='type']/text()").extract_first()
            if 'news' in type.strip().lower():
                item = NewsItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'news'
                item['target'] = 'UN Environment Programme'
                item['language'] = 'en'
                item['domain'] = 'www.unenvironment.org'
                abstract = response.xpath(
                    ".//div[@class='result_item_summary']/p/text()").extract_first()
                if abstract:
                    item['abstract'] = abstract.strip()
                publish_time = response.xpath(
                    ".//span[@class='date']/text()").extract_first()
                if publish_time:
                    item['publish_time'] = datetime.strptime(
                        publish_time.strip(), "%d %b %Y")
                url = box.xpath(
                    ".//div[@class='result_item_title']/h5/a/@href").extract_first()
                item['url'] = 'https://www.unenvironment.org' + url
                yield scrapy.Request(item['url'], callback=self.news_detail_parser, meta={'item': item.copy()})

        next_page = response.xpath("//a[@rel='next']/@href").extract_first()
        if next_page:
            next_page = "https://www.unenvironment.org/resources" + next_page
            yield scrapy.Request(next_page, callback=self.news_parser)

    def news_detail_parser(self, response):
        item = response.meta.get('item')
        item['title'] = response.xpath(
            "//div[@class='article_header_meta_title']/h1/text()").extract_first()
        item['content_html'] = response.xpath("//article").extract_first()
        item['content'] = response.xpath("string(//article)").extract_first()
        item['init_time'] = datetime.now()
        keyword = response.xpath(
            "//div[@class='article_tags']//a/text()").extract()
        item['keyword'] = ";".join(keyword)
        item['uuid'] = str(uuid.uuid1())
        yield item

    def wedocdownload(self, response):
        item = response.meta.get('item')
        link = response.xpath(
            "//div[contains(@class,'pull-left')]//a/@href").extract_first()
        link = "https://wedocs.unep.org/" + link
        item['file_urls'] = [link]
        yield item


class AfricaPortalSpider(scrapy.Spider):
    name = 'africa_portal'
    allowed_domains = ['africaportal.org']
    start_urls = ["https://www.africaportal.org/publications/?page=1"]

    def parse(self, response):
        boxes = response.xpath(
            "//div[@class='c-feature-list']//a[contains(@class,'article')]")
        for box in boxes:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            url = box.xpath("./@href").extract_first()
            item['url'] = 'https://www.africaportal.org' + url
            item['target'] = 'www.africaportal.org'
            item['language'] = 'en'
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

        next_page = response.xpath(
            "//a[contains(text(),'Next page')]/@href").extract_first()
        if next_page:
            next_page = "https://www.africaportal.org/publications/" + next_page
            yield scrapy.Request(next_page, callback=self.parse)

    def detail_parser(self, response):
        item = response.meta.get('item')
        author = response.xpath(
            "//p[@class='c-meta-pub__author']/a[@class='a-text--link']/text()").extract_first()
        item['author'] = author.strip() if author else None
        publish_time = response.xpath(
            "//p[@class='c-meta-pub__date']/text()").extract_first().strip()
        if publish_time:
            item['publish_time'] = datetime.strptime(publish_time, '%d %b %Y')
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
        item['init_time'] = datetime.now()
        item['pages'] = int(response.xpath(
            "//div[@class='c-meta-pub__length']/div[@class='text']/span[1]/text()").extract_first().strip())
        # item['organization'] = response.xpath(
        #    "//li[contains(text(),'Content partner')]/following-sibling::li[1]/a/text()").extract_first()
        item['organization'] = '非洲门户'
        location = response.xpath(
            "//li[contains(text(),'Region')]/following-sibling::li[1]/a/text()").extract()
        item['location'] = ",".join(location)
        keywords = response.xpath(
            "//li[contains(text(),'Subject')]/following-sibling::li/a/text()").extract()
        item['keyword'] = ";".join(keywords)
        item['title'] = response.xpath(
            "//h1[@class='a-text--publication-title']/text()").extract_first().strip()
        issue = re.findall(r"Issue No (\d+)", item['title'])
        if issue:
            item['issue'] = issue[0]
        item['abstract'] = response.xpath(
            "//div[@class='rich-text']/p/text()").extract_first().strip()
        item['content'] = response.xpath(
            "//div[@class='rich-text']/p/text()").extract_first().strip()
        file_link = response.xpath(
            "//a[@class='button--primary']/@href").extract_first()
        if file_link:
            file_link = 'https://www.africaportal.org' + file_link
            item['file_urls'] = [file_link]
        item['uuid'] = str(uuid.uuid1())
        yield item


class WeforumSpider(scrapy.Spider):
    name = 'weforum'
    allowed_domains = ['weforum.org']
    start_urls = ['https://www.weforum.org/reports']

    def parse(self, response):
        boxes = response.xpath(
            "//div[@class='row page']//article[@class='tout tout--default tout--report']")
        for box in boxes:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['title'] = box.xpath(
                ".//h3[@class='tout__title']/text()").extract_first()
            item['url'] = "https://www.weforum.org" + \
                box.xpath(".//a[@class='tout__link']/@href").extract_first()
            item['target'] = 'www.weforum.org'
            item['organization'] = '世界经济论坛'
            item['language'] = 'en'
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

        # 翻页
        next = response.xpath("//a[@rel='next']/@href").extract_first()
        if next:
            next_url = 'https://www.weforum.org' + next
            yield scrapy.Request(next_url, callback=self.parse)

    def detail_parser(self, response):
        item = response.meta.get('item')
        publish_time = response.xpath(
            "//div[@class='report__meta']/div[@class='caption']/text()").extract_first()
        if publish_time:
            item['publish_time'] = datetime.strptime(publish_time, "%d %B %Y")
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
        content = response.xpath(
            "//div[contains(@class,'st__content-block--text')]")
        content = content.xpath("string(.)").extract()
        content = "".join(content)
        item['content'] = content.strip()
        item['abstract'] = content.strip()
        item['init_time'] = datetime.now()
        keywords = rake.run(content)
        keywords = [tuple[0] for tuple in keywords[:3]]
        item['keyword'] = ';'.join(keywords)
        file_link = response.xpath(
            "//a[@class='report__link--pdf']/@href").extract_first()
        item['file_urls'] = [file_link]
        item['uuid'] = str(uuid.uuid1())
        yield item


class NistSpider(scrapy.Spider):
    name = 'nist'
    allowed_domains = ['nist.gov', 'scitation.org',
                       'wiley.com', 'doi.org', 'springer.com', 'nature.com']
    start_urls = [
        'https://www.nist.gov/publications/search?k=&t=&a=&s=All&n=&d%5Bmin%5D=&d%5Bmax%5D=&page=1']

    def parse(self, response):
        boxes = response.xpath("//article[@class='nist-teaser']")
        for box in boxes:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'en'
            item['organization'] = '美国国家标准与技术研究院'
            item['target'] = 'www.nist.gov'
            item['title'] = box.xpath(".//h3/a/span/text()").extract_first()
            url = box.xpath(".//h3/a/@href").extract_first()
            item['url'] = 'https://www.nist.gov' + url
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})
        # 翻页
        next = response.xpath("//a[@rel='next']/@href").extract_first()
        if next:
            next_url = 'https://www.nist.gov' + next
            yield scrapy.Request(next_url, callback=self.parse)

    def detail_parser(self, response):
        item = response.meta.get('item')
        item['uuid'] = str(uuid.uuid1())
        publish_time = response.xpath('//time/@datetime').extract_first()
        if publish_time:
            item['publish_time'] = datetime.strptime(
                publish_time, "%Y-%m-%dT%H:%M:%SZ")
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
        item['init_time'] = datetime.now()
        authors = response.xpath(
            """//div[@data-layout-content-preview-placeholder-label='"Author(s)" field']//div[@class='nist-field__item']//text()""").extract()
        if authors:
            authors = "".join(authors)
            authors = authors.split(",")
            item['author'] = ";".join([author.strip() for author in authors])
        abstract = response.xpath(
            "//div[@class='text-with-summary']/text()").extract()
        item['abstract'] = "".join([ab.strip() for ab in abstract])
        item['content'] = item['abstract']
        keyword = response.xpath(
            "//h3[contains(text(),'Keywords')]/following-sibling::div/text()").extract_first()
        item['keyword'] = ";".join(
            [k.strip() for k in keyword.split(",")]) if keyword else None
        volume = response.xpath(
            "//div[contains(text(),'Volume')]/following-sibling::div/text()").extract_first()
        issue = response.xpath(
            "//div[contains(text(),'Issue')]/following-sibling::div/text()").extract_first()
        journals_title = response.xpath(
            "//div[contains(text(),'Citation')]/following-sibling::div/text()").extract_first()
        item['volume'] = volume if volume else None
        item['issue'] = issue if issue else None
        item['journals_title'] = journals_title if journals_title else None
        item['category'] = 'report'
        location = response.xpath(
            "//div[contains(text(),'Location')]/following-sibling::div/text()").extract_first()
        item['location'] = location if location else None
        item['project'] = 1
        file_link = response.xpath(
            "//h3[contains(text(),'Download')]/following-sibling::div//a/@href").extract()
        if file_link and len(file_link) > 1:
            for link in file_link:
                if "get_pdf.cfm" in link:
                    file_link = link
                    break
            else:
                file_link = file_link[0]
            yield scrapy.Request(file_link, callback=self.headers_parser,
                                 meta={'item': item.copy(), 'dont_redirect': True,
                                       'handle_httpstatus_list': [200, 404, 301, 302]}, method='HEAD')
        elif file_link and len(file_link) == 1:
            yield scrapy.Request(file_link[0], callback=self.headers_parser, meta={'item': item.copy(), 'dont_redirect': True, 'handle_httpstatus_list': [200, 404, 301, 302]}, method='HEAD')
        else:
            yield item

    def headers_parser(self, response):
        item = response.meta.get('item')
        headers = response.headers.to_unicode_dict()
        if 'location' in headers or 'Location' in headers:
            if 'nature.com' in headers['location']:
                item['file_urls'] = [headers['location'] + '.pdf']
            elif 'mdpi.com' in headers['location']:
                item['file_urls'] = [headers['location'] + '/pdf']
            elif 'pubs.acs.org' in headers['location']:
                yield scrapy.Request(response.url, callback=self.acs_parser, meta={'item': item.copy()})
                return
            else:
                item['file_urls'] = [headers['location']]
        else:
            item['file_urls'] = [response.url]
        yield item

    def acs_parser(self, response):
        item = response.meta.get('item')
        file_link = response.xpath(
            "//a[@class='suppl-anchor']/@href").extract_first()
        if file_link:
            file_link = "https://pubs.acs.org" + file_link
            item['file_urls'] = [file_link]
        yield item


class UseitSpider(scrapy.Spider):
    name = 'useit'
    allowed_domains = ['useit.com.cn']
    custom_settings = {
        'DOWNLOAD_DELAY': 2.5,
    }
    start_urls = [
        'https://www.useit.com.cn/forum-299-1.html',
        'https://www.useit.com.cn/forum-286-1.html',
        'https://www.useit.com.cn/forum-345-1.html',
        'https://www.useit.com.cn/forum-120-1.html',
        'https://www.useit.com.cn/forum-277-1.html',
        'https://www.useit.com.cn/forum-347-1.html',
        'https://www.useit.com.cn/forum-319-1.html',
        'https://www.useit.com.cn/forum-294-1.html',
        'https://www.useit.com.cn/forum-318-1.html'
    ]

    def parse(self, response):
        boxes = response.xpath("//ul[@id='waterfall']/li")
        for box in boxes:
            item = ReportItem()
            item['title'] = box.xpath(".//h3/a/text()").extract_first()
            item['url'] = box.xpath(".//h3/a/@href").extract_first()
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

        # 翻页
        next = response.xpath("//a[@class='nxt']/@href").extract_first()
        if next:
            yield scrapy.Request(next, callback=self.parse)

    def detail_parser(self, response):
        item = response.meta.get('item')
        keyword = response.xpath(
            "//div[@class='ptg mbm mtn']/a/text()").extract()
        item['keyword'] = ','.join(keyword)
        head_string = response.xpath("string(//font)").extract_first()
        publish_time = re.findall(r"分享时间：(.*?)\|", head_string)
        if publish_time:
            item['publish_time'] = datetime.strptime(
                publish_time[0].strip(), "%Y-%m-%d %H:%M")
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
        item['init_time'] = datetime.now()
        item['category'] = 'report'
        item['language'] = 'zh'
        item['target'] = 'www.useit.com.cn'
        item['crawler_ip'] = get_host_ip()
        item['uuid'] = str(uuid.uuid1())
        item['project'] = 1
        if '：' in item['title']:
            item['organization'] = item['title'].split('：')[0]
        else:
            item['organization'] = "Useit知识库"
        content = response.xpath("//td[@class='t_f']/text()").extract()
        for x in range(content.count('\r\n')):
            content.remove("\r\n")
        item['abstract'] = "\r\n".join(content)
        if item['abstract'] == "":
            else_content = response.xpath(
                "//td[@class='t_f']/div/text()").extract()
            for x in range(else_content.count('\r\n')):
                else_content.remove("\r\n")
            item['abstract'] = "\r\n".join(else_content)
        if item['abstract'] == "":
            else_content2 = response.xpath(
                "//td[@class='t_f']/p/*[not(@class='xs0')]/text()").extract()
            for x in range(else_content2.count('\r\n')):
                else_content2.remove("\r\n")
            item['abstract'] = "\r\n".join(else_content2)
        if item['abstract'] == "":
            else_content3 = response.xpath(
                "//td[@class='t_f']//font/text()").extract()
            for x in range(else_content3.count('\r\n')):
                else_content3.remove("\r\n")
            item['abstract'] = "\r\n".join(else_content3)
        item['content'] = item['abstract']
        imgs = response.xpath(
            "//div[@class='xs0']/a[contains(text(),'下载本地')]/@href").extract()
        item['file_urls'] = imgs
        yield item


class DeloitteSpider(scrapy.Spider):
    name = 'deloitte'
    allowed_domains = ['deloitte.com']
    start_urls = [
        'https://www2.deloitte.com/cn/zh/pages/energy-and-resources/solutions/publications-enr.html',
        'https://www2.deloitte.com/cn/zh/pages/consumer-industrial-products/solutions/publications-chemical.html',
        'https://www2.deloitte.com/cn/zh/pages/life-sciences-and-healthcare/solutions/publications.html?icid=nav2_publications#',
        'https://www2.deloitte.com/cn/zh/pages/technology-media-and-telecommunications/solutions/publications.html'
    ]

    def parse(self, response):
        lis = response.xpath(
            "//div[@class='standardcopy parbase section']//ul/li")
        for li in lis:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'zh'
            item['organization'] = '德勤'
            item['target'] = 'www2.deloitte.com'
            item['project'] = 1
            item['title'] = li.xpath("./a/text()").extract_first()
            url = li.xpath("./a/@href").extract_first()
            item['url'] = 'https://www2.deloitte.com' + url
            if item['url'].endswith('.pdf'):
                item['file_urls'] = [item['url']]
                item['init_time'] = datetime.now()
                abstract = li.xpath("./text()").extract()
                item['abstract'] = "".join(abstract)
                item['content'] = "".join(abstract)
                yield item
            else:
                yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

    def detail_parser(self, response):
        item = response.meta.get('item')
        subtitle = response.xpath(
            "//h2[@class='secondary-headline']/text()").extract_first()
        if subtitle:
            item['subtitle'] = subtitle
        publish_time = re.findall(r"出版日期：\d+年\d+月\d+日", response.text)
        if publish_time:
            item['publish_time'] = datetime.strptime(
                publish_time[0], "出版日期：%Y年%m月%d日")
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
        item['init_time'] = datetime.now()
        item['abstract'] = response.xpath(
            "string(//div[@class='custom-rte'])").extract_first().strip()
        item['content'] = item['abstract']
        keyword = response.xpath(
            "//ul[@class='article-tags']//a/text()").extract()
        item['keyword'] = ";".join(
            [",".join(word.split('、')) for word in keyword])
        item['uuid'] = str(uuid.uuid1())
        file_url = response.xpath("//a[@download]/@href").extract_first()
        if file_url:
            item['file_urls'] = ['https://www2.deloitte.com' + file_url]
        yield item


class CaictSpider(scrapy.Spider):
    name = 'caict'
    allowed_domains = ['caict.ac.cn']
    start_urls = [
        'http://www.caict.ac.cn/kxyj/qwfb/bps/index.htm',
        'http://www.caict.ac.cn/kxyj/qwfb/bps/index_1.htm',
        'http://www.caict.ac.cn/kxyj/qwfb/bps/index_2.htm',
        'http://www.caict.ac.cn/kxyj/qwfb/bps/index_3.htm',
        'http://www.caict.ac.cn/kxyj/qwfb/bps/index_4.htm',
        'http://www.caict.ac.cn/kxyj/qwfb/bps/index_5.htm',
        'http://www.caict.ac.cn/kxyj/qwfb/bps/index_6.htm',
        'http://www.caict.ac.cn/kxyj/qwfb/bps/index_7.htm',
        'http://www.caict.ac.cn/kxyj/qwfb/bps/index_8.htm',
        'http://www.caict.ac.cn/kxyj/qwfb/bps/index_9.htm',
        'http://www.caict.ac.cn/kxyj/qwfb/bps/index_10.htm',
    ]

    def parse(self, response):
        trs = response.xpath("//table//table//table//table//table/tbody/tr")
        for tr in trs:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'zh'
            item['organization'] = '中国信息通信研究院'
            item['target'] = 'www.caict.ac.cn'
            item['project'] = 1
            title = tr.xpath("./td[1]//a/text()").extract_first()
            if title:
                item['title'] = title
                publish_time = tr.xpath("./td[2]//text()").extract_first()
                item['publish_time'] = datetime.strptime(
                    publish_time, "%Y-%m-%d")
                item['year'] = datetime.strftime(item['publish_time'], "%Y")
                item['init_time'] = datetime.now()
                url = tr.xpath("./td[1]//a/@href").extract_first()
                item['url'] = 'http://www.caict.ac.cn/kxyj/qwfb/bps' + url[1:]
                yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

    def detail_parser(self, response):
        item = response.meta.get('item')
        item['uuid'] = str(uuid.uuid1())
        abstract = response.xpath(
            "//div[@class='pagemaintext']/text()").extract()
        if abstract:
            item['abstract'] = abstract[0]
            item['content'] = item['abstract']
            keyword = extract_tags(
                item['abstract'], topK=3, allowPOS=('n', 'v', 'vn'))
            item['keyword'] = ";".join(keyword)
        file_link_tale = response.xpath(
            "//div[@class='pagemaintext']/a/@href").extract_first()
        if file_link_tale:
            file_link_head = re.findall(r"(.*)/.*$", response.url)[0]
            file_link = file_link_head + file_link_tale[1:]
            item['file_urls'] = [file_link]
        yield item


class IyiouSpider(scrapy.Spider):
    name = 'iyiou'
    allowed_domains = ['iyiou.com']
    start_urls = [
        'https://www.iyiou.com/search?p=%E9%99%A2%E5%A3%AB&page=1',
        'https://www.iyiou.com/search?p=%E9%99%A2%E5%A3%AB&page=2',
        'https://www.iyiou.com/search?p=%E9%99%A2%E5%A3%AB&page=3',
        'https://www.iyiou.com/search?p=%E9%99%A2%E5%A3%AB&page=4',
        'https://www.iyiou.com/search?p=%E9%99%A2%E5%A3%AB&page=5',
        'https://www.iyiou.com/search?p=%E9%99%A2%E5%A3%AB&page=6',
        'https://www.iyiou.com/search?p=%E9%99%A2%E5%A3%AB&page=7',
        'https://www.iyiou.com/search?p=%E9%99%A2%E5%A3%AB&page=8',
    ]

    def parse(self, response):
        lis = response.xpath("//ul[@class='newestArticleList']/li")
        for li in lis:
            item = NewsItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'news'
            item['language'] = 'zh'
            item['domain'] = 'www.iyiou.com'
            item['target'] = '亿欧'
            item['project'] = 1
            title = li.xpath('.//a/@title').extract_first()
            title = title.replace('<em>', '')
            item['title'] = title.replace('</em>', '')
            item['url'] = li.xpath('.//a/@href').extract_first()
            author = li.xpath(".//span[@class='name']/text()").extract_first()
            if author:
                item['author'] = author.replace('  ·  ', '').strip()
            yield scrapy.Request(item['url'], self.detail_parser, meta={'item': item.copy()})

    def detail_parser(self, response):
        item = response.meta.get('item')
        item['uuid'] = str(uuid.uuid1())
        item['source'] = response.xpath(
            "//div[@id='post_source']/text()").extract_first()
        publish_time = response.xpath(
            "//div[@id='post_date']/text()").extract_first()
        item['publish_time'] = datetime.strptime(
            publish_time, "%Y-%m-%d · %H:%M")
        item['init_time'] = datetime.now()
        html = response.xpath(
            "//div[@id='post_description']//*[not(name()='div') and not(name()='a') and not(name()='select') and not(name()='option') and not(name()='span') and not(@style='color:#666;')]").extract()
        html = "".join(html).strip()
        r = re.findall(r"(.*)<hr>(.*)", html, re.S)
        if r:
            # 有分割线 可分出内容与简介
            abstract = r[0][0]
            contentb = r[0][1]
            abstract_html = etree.HTML(abstract)
            content_html = etree.HTML(contentb)

            abstract = abstract_html.xpath("//text()")
            abstract = "".join(abstract).strip()
            content_html = content_html.xpath("//text()")
            content_html = "".join(content_html)

            content = etree.HTML(content_html)
            content = content.xpath("//text()")
            content = "".join(content)
            item['content'] = content
            item['abstract'] = abstract
            item['content_html'] = contentb
        else:
            item['content_html'] = html
            content = response.xpath(
                "//div[@id='post_description']//*[not(name()='div') and not(name()='a') and not(name()='select') and not(name()='option') and not(name()='span') and not(@style='color:#666;') and not(name()='blockquote')]/text()").extract()
            content = "".join(content).strip()
            item['content'] = content

        keyword = response.xpath(
            "//div[@class='article_info_box tags']//a/text()").extract()
        item['keyword'] = ";".join(keyword)
        yield item


class CiotimesSpider(scrapy.Spider):
    name = 'ciotimes'
    allowed_domains = ['ciotimes.com']
    start_urls = [
        'http://www.ciotimes.com/index.php?m=search&c=index&a=init&typeid=1&q=%E9%99%A2%E5%A3%AB&siteid=1&time=year&page=1']

    def parse(self, response):
        lis = response.xpath(
            "//div[@class='col-md-9 col-sm-9 col-xs-12 c']//ul/li")
        for li in lis:
            item = NewsItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'news'
            item['language'] = 'zh'
            item['domain'] = 'http://www.ciotimes.com/'
            item['target'] = 'CIO时代'
            item['project'] = 1
            title = li.xpath(".//h5/a//text()").extract()
            item['title'] = "".join(title)
            item['url'] = li.xpath(".//h5/a/@href").extract_first()
            abstract = li.xpath("./div/p//text()").extract()
            item['abstract'] = "".join(abstract)
            publish_time = li.xpath(
                "./div[@class='adds']/text()").extract_first()
            item['publish_time'] = datetime.strptime(
                publish_time, "发布时间：%Y-%m-%d %H:%M:%S")
            item['init_time'] = datetime.now()
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})
        # 翻页
        if response.url.endswith("page=1"):
            rows = response.xpath("//a[@class='a1'][1]/text()").extract_first()
            rows = re.findall("\d+", rows)
            if rows:
                rows_count = int(rows[0])
                page_count = int(rows_count / 10)
                for p in range(2, page_count + 1):
                    yield scrapy.Request("http://www.ciotimes.com/index.php?m=search&c=index&a=init&typeid=1&q=%E9%99%A2%E5%A3%AB&siteid=1&time=year&page={}".format(p),
                                         callback=self.parse)

    def detail_parser(self, response):
        item = response.meta.get('item')
        item['uuid'] = str(uuid.uuid1())
        source = response.xpath(
            "//p[@class='ly visible-xs text-left']/text()").extract_first()
        try:
            source = re.findall(r".*来源：(.*)", source)
        except:
            soucre = None
        if source:
            item['source'] = source[0]
        keyword = response.xpath("//small[@class='gjz']/a/text()").extract()
        if keyword:
            item['keyword'] = ";".join(keyword)
        content_html = response.xpath(
            "//div[@id='cont']/*[not(contains(@style,'color:blue;text-decoration:underline;'))]").extract()
        item['content_html'] = "".join(content_html)
        content1 = response.xpath("//div[@id='cont']/text()").extract()
        content1 = "".join([x.strip() for x in content1]).strip()
        content2 = response.xpath(
            "//div[@id='cont']/*[not(contains(@style,'color:blue;text-decoration:underline;'))]//text()").extract()
        content2 = "\n".join([x.strip() for x in content2]).strip()
        content = content1 + content2
        item['content'] = content
        yield item


class RffSpider(scrapy.Spider):
    name = 'rff'
    allowed_domains = ['rff.org']
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36'
    }

    def start_requests(self):
        url = "https://www.rff.org/api/v2/pages/?base_type=core.BasePage&fields=introduction%2Cpublication_type%28title%29%2Cpublication_cover_fill_197x254&limit=12&offset=0&order=-first_published_at&publication_type=14&type=publications.PublicationPage"
        yield scrapy.Request(url, callback=self.parse, headers=self.headers)

    def parse(self, response):
        d = json.loads(response.text)
        reports = d['items']
        if reports:
            for report in reports:
                item = ReportItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'report'
                item['language'] = 'en'
                item['organization'] = '未来的资源'
                item['target'] = 'www.rff.org'
                item['project'] = 1
                publish_time_str = report['meta']['first_published_at']
                item['url'] = report['meta']['html_url']
                item['title'] = report['title']
                item['abstract'] = report['introduction']
                publish_time = datetime.strptime(
                    publish_time_str[:-6], "%Y-%m-%dT%H:%M:%S")
                item['publish_time'] = publish_time
                item['year'] = datetime.strftime(item['publish_time'], "%Y")
                item['init_time'] = datetime.now()
                yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

            # 接口翻页
            this_page_offset = int(re.findall(
                r"offset=(\d+)", response.url)[0])
            next_page_offset = this_page_offset + 12
            next_url = "https://www.rff.org/api/v2/pages/?base_type=core.BasePage&fields=introduction%2Cpublication_type%28title%29%2Cpublication_cover_fill_197x254&limit=12&offset={}&order=-first_published_at&publication_type=14&type=publications.PublicationPage".format(
                next_page_offset)
            yield scrapy.Request(next_url, callback=self.parse, headers=self.headers)

    def detail_parser(self, response):
        item = response.meta.get('item')
        item['uuid'] = str(uuid.uuid1())
        content = response.xpath("//div[@class='rich-text']//text()").extract()
        if content:
            item['content'] = "\n".join(content)
        else:
            item['content'] = item['abstract']
        keyword = response.xpath(
            "//div[@class='tags-list']//li/a/text()").extract()
        if keyword:
            item['keyword'] = ";".join([x.strip() for x in keyword]).strip()
        authors = response.xpath(
            "//div[@class='card-small__wrapper']/h2/text()").extract()
        if authors:
            item['author'] = ";".join(authors)
        file_link = response.xpath(
            "//a[contains(@class,'hero-publication__button')]/@href").extract_first()
        if file_link and file_link.lower().endswith(".pdf"):
            if file_link.startswith("/documents"):
                file_link = "https://www.rff.org" + file_link
            item['file_urls'] = [file_link]
        yield item


class ChathamHouseSpider(scrapy.Spider):
    name = 'chathamhouse'
    allowed_domains = ['chathamhouse.org']
    start_urls = [
        'https://www.chathamhouse.org/research/publications/all?researchnav',
        'https://www.chathamhouse.org/experts/comment?expertsnav'
    ]

    def parse(self, response):
        if 'publications' in response.url:
            boxes = response.xpath("//div[@class='teasers__wrapper ']/a")
            for box in boxes:
                item = ReportItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'report'
                item['language'] = 'en'
                item['organization'] = '查塔姆研究所'
                item['target'] = 'www.chathamhouse.org'
                item['project'] = 1
                url = box.xpath("./@href").extract_first()
                item['url'] = "https://www.chathamhouse.org" + url
                item['title'] = box.xpath(".//h3/text()").extract_first()
                publish_time = box.xpath(
                    ".//div[@class='teaser__description__date']/text()").extract_first().strip()
                item['publish_time'] = datetime.strptime(
                    publish_time, "%d %B %Y")
                item['year'] = datetime.strftime(item['publish_time'], "%Y")
                item['init_time'] = datetime.now()
                abstract = box.xpath(
                    ".//div[@class='teaser__description']/text()").extract()
                item['abstract'] = abstract[1].strip()
                yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

            # 翻页
            next = response.xpath(
                "//a[@title='Go to next page']/@href").extract_first()
            if next:
                next = "https://www.chathamhouse.org" + next
                yield scrapy.Request(next, callback=self.parse)
        else:
            a_s = response.xpath("//div[@class='teasers__wrapper ']/a")
            for a in a_s:
                item = NewsItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'news'
                item['language'] = 'en'
                item['target'] = '查塔姆研究所'
                item['domain'] = 'www.chathamhouse.org'
                item['project'] = 1
                item['uuid'] = str(uuid.uuid1())
                item['title'] = a.xpath(".//h3/text()").extract_first()
                url = a.xpath("./@href").extract_first()
                item['url'] = "https://www.chathamhouse.org" + url
                publish_time = a.xpath(
                    ".//div[@class='teaser__description__date']/text()").extract_first()
                if publish_time:
                    item['publish_time'] = datetime.strptime(
                        publish_time.strip(), '%d %B %Y')
                item['init_time'] = datetime.now()
                abstract = a.xpath(
                    "./div[@class='teaser__wrapper']/div[@class='teaser__content']/div[@class='teaser__description']/text()[2]").extract_first()
                if abstract:
                    item['abstract'] = abstract.strip()
                yield scrapy.Request(item['url'], callback=self.news_detail_parser, meta={'item': item.copy()})
            next = response.xpath(
                "//a[@title='Go to next page']/@href").extract_first()
            if next:
                next = "https://www.chathamhouse.org" + next
                yield scrapy.Request(next, callback=self.parse)

    def news_detail_parser(self, response):
        item = response.meta.get('item')
        content = response.xpath(
            "string(//div[@class='body rich-text'])").extract_first()
        if content:
            item['content'] = content.strip()
        item['content_html'] = response.xpath(
            "//div[@class='body rich-text']").extract_first()
        keyword = response.xpath(
            "//div[contains(@class,'topic')]//div[@class='field-content']/a/text()").extract()
        item['keyword'] = ";".join(keyword)
        item['author'] = response.xpath(
            "//div[@class='author']//h3/a/text()").extract_first()
        yield item

    def detail_parser(self, response):
        item = response.meta.get('item')
        item['uuid'] = str(uuid.uuid1())
        # content = response.xpath("//div[@class='body rich-text']//strong[contains(text(),'Summary')]/../following-sibling::ul//text()").extract()
        content = response.xpath(
            "//div[@class='body rich-text']//text()").extract()
        content = "\n".join([x.strip() for x in content])
        summary_title = re.findall(r"(^\s+Summary)", content)
        if summary_title:
            content = content.replace(summary_title[0], "")
        item['content'] = content
        if content:
            item['abstract'] = item['content']
        keyword = response.xpath(
            "//span[@class='views-label views-label-field-topics']/following-sibling::div/a/text()").extract()
        item['keyword'] = ";".join(keyword)
        file_link = response.xpath(
            "//a[@class='file-link-wrapper']/@href").extract_first()
        item['file_urls'] = [file_link]
        author = response.xpath(
            "//h3[@class='author__heading']//text()").extract()
        author = [x.strip() for x in author]
        authors = []
        for a in author:
            if a:
                authors.append(a)
        item['author'] = ";".join(authors)
        yield item


class SeiSpider(scrapy.Spider):
    name = 'sei'
    allowed_domains = ['sei.org', 'doi.org',
                       'springer.com', 'mdpi.com', 'tandfonline.com']
    start_urls = ['https://www.sei.org/publications/',
                  'https://www.sei.org/perspectives/']

    def parse(self, response):
        if 'publications' in response.url:
            boxes = response.xpath("//div[@class='c-content-item__container']")
            for box in boxes:
                item = ReportItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'report'
                item['language'] = 'en'
                item['organization'] = '斯德哥尔摩环境研究所'
                item['target'] = 'www.sei.org'
                item['project'] = 1
                item['url'] = box.xpath(
                    ".//a[@class='c-content-item__title-link']/@href").extract_first()
                item['title'] = box.xpath(
                    ".//a[@class='c-content-item__title-link']/text()").extract_first()
                yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})
        else:
            boxes = response.xpath("//div[@class='c-content-item__container']")
            for box in boxes:
                item = NewsItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'news'
                item['language'] = 'en'
                item['target'] = '斯德哥尔摩环境研究所'
                item['domain'] = 'www.sei.org'
                item['project'] = 1
                item['url'] = box.xpath(
                    ".//a[@class='c-content-item__title-link']/@href").extract_first()
                item['title'] = box.xpath(
                    ".//a[@class='c-content-item__title-link']/text()").extract_first()
                yield scrapy.Request(item['url'], callback=self.news_parser, meta={'item': item.copy()})
        # 翻页
        next = response.xpath("//a[@title='Next page']/@href").extract_first()
        if next:
            yield scrapy.Request(next, callback=self.parse)
    def detail_parser(self, response):
        item = response.meta.get('item')
        item['uuid'] = str(uuid.uuid1())
        publish_time = response.xpath(
            "//span[@class='c-content-meta__text c-content-meta__text--published-date']/span[2]/text()").extract_first()
        if publish_time:
            item['publish_time'] = datetime.strptime(
                publish_time.strip(), "%d %B %Y")
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
        item['init_time'] = datetime.now()
        abstract = response.xpath(
            "//h2[@id='introduction']/following-sibling::*//text()").extract()
        if not abstract:
            abstract = response.xpath(
                "//div[@class='ts-body content-from-editor ']/*//text()").extract()
        item['abstract'] = "\n".join(abstract)
        item['content'] = item['abstract']
        author = response.xpath(
            "//div[@class='c-bar-author__content']/a/text()").extract()
        item['author'] = ";".join(author)
        keyword = response.xpath("//a[@class='c-bar__item']/text()").extract()
        if keyword:
            item['keyword'] = ";".join(keyword)
        file_link = response.xpath(
            "//a[contains(@class,'download')]/@href").extract_first()
        if file_link:
            item['file_urls'] = [file_link]
            yield item
        else:
            open_access = response.xpath(
                "//span[contains(@class,'c-icon--open')]").extract()
            if open_access:
                file_link = response.xpath(
                    "//a[contains(@class,'c-content-meta-buttons__button--access')]/@href").extract_first()
                yield scrapy.Request(file_link, callback=self.file_parser, meta={'item': item.copy()})

    def file_parser(self, response):
        item = response.meta.get('item')
        if 'sciencedirect' in response.url:
            file_link = response.xpath(
                "//span[contains(text(),'Download this article')]/../@href").extract_first()
            if file_link:
                file_link = 'https://www.sciencedirect.com' + file_link
                item['file_urls'] = [file_link]
        elif 'cambridge' in response.url:
            file_link = response.xpath(
                "//a[@aria-label='Download PDF for this Article']/@href").extract_first()
            if file_link:
                file_link = 'https://www.cambridge.org' + file_link
                item['file_urls'] = [file_link]
        elif 'mdpi' in response.url:
            file_link = response.xpath(
                "//a[contains(text(),'Download PDF')]/@href").extract_first()
            if file_link:
                file_link = 'https://www.mdpi.com' + file_link
                item['file_urls'] = [file_link]
        elif 'iopscience' in response.url:
            file_link = response.xpath(
                "//div[@class='btn-multi-block mb-1']//span[contains(text(),'Download')]/../@href").extract_first()
            if file_link:
                file_link = 'https://iopscience.iop.org' + file_link
                item['file_urls'] = [file_link]
        elif 'biomedcentral' in response.url:
            file_link = response.xpath(
                "//div[contains(@class,'c-pdf-download')]/a/@href").extract_first()
            if file_link:
                file_link = 'https:' + file_link
                item['file_urls'] = [file_link]
        yield item

    def news_parser(self, response):
        item = response.meta.get('item')
        item['uuid'] = str(uuid.uuid1())
        publish_time = response.xpath(
            "//span[@class='c-content-meta__text c-content-meta__text--published-date']/span[2]/text()").extract_first()
        if publish_time:
            item['publish_time'] = datetime.strptime(
                publish_time.strip(), "%d %B %Y")
        item['init_time'] = datetime.now()
        abstract = response.xpath(
            "//div[contains(@class,'c-content-header__intro')]//text()").extract()
        abstract_l = []
        for i in [x.strip() for x in abstract]:
            if i:
                abstract_l.append(i)
        item['abstract'] = "\n".join(abstract_l)
        author = response.xpath(
            "//h5[contains(text(),'Written by')]/following-sibling::div//a/text()").extract()
        author_l = []
        for a in [x.strip() for x in author]:
            if a:
                author_l.append(a)
        item['author'] = ";".join(author_l)
        content_html = response.xpath(
            "//article[@role='main']/*[not(contains(@class,'people-list'))]").extract()
        item['content_html'] = "".join(content_html)
        content = response.xpath(
            "//div[@class='c-title-aside-layout__content']/div[@class='ts-body content-from-editor ']//text()").extract()
        for c in content.copy():
            if not c.strip():
                content.remove(c)
            else:
                break
        item['content'] = "".join(content)
        yield item


class NberSpider(scrapy.Spider):
    name = 'nber'
    allowed_domains = ['nber.org']
    start_urls = ['https://www.nber.org/2009_redesign/archive3.pl']

    def parse(self, response):
        boxes = response.xpath("//div[@class='mainStory']")
        for box in boxes:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'en'
            item['organization'] = '美国国家经济研究局'
            item['target'] = 'www.nber.org'
            item['project'] = 1
            publish_time = box.xpath(
                "./p[@class='subheadSource']/text()").extract_first().strip()
            if publish_time:
                item['publish_time'] = datetime.strptime(
                    publish_time, "%d %B %Y")
                item['year'] = datetime.strftime(item['publish_time'], "%Y")
            item['init_time'] = datetime.now()
            item['title'] = box.xpath(
                "./h2[@class='subheadHead']/a/text()").extract_first().strip()
            url = box.xpath(
                "./h2[@class='subheadHead']/a/@href").extract_first().strip()
            item['url'] = "https://www.nber.org" + url
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

        # 翻页
        next = response.xpath(
            "//a[contains(text(),'Continue to Earlier Research')]/@href").extract_first()
        if next:
            next = "https://www.nber.org" + next
            yield scrapy.Request(next, callback=self.parse)

    def detail_parser(self, response):
        item = response.meta.get('item')
        item['uuid'] = str(uuid.uuid1())
        author = response.xpath(
            "//h2[@class='bibtop citation_author']/a/text()").extract()
        item['author'] = ";".join(author)
        item['abstract'] = response.xpath(
            "//td[@id='mainContentTd']/p[2]/text()").extract_first().strip()
        item['content'] = item['abstract']
        keywords = rake.run(item['content'])
        keywords = [tuple[0] for tuple in keywords[:3]]
        item['keyword'] = ';'.join(keywords)
        file_link = re.sub(r"\?sy=\d+$", ".pdf", response.url)
        item['file_urls'] = [file_link]
        yield item


class CarnegieendowmentSpider(scrapy.Spider):
    name = "carnegieendowment"
    allowed_domains = ['carnegieendowment.org', 'carnegie-mec.org',
                       'carnegie.ru', 'carnegieeurope.eu', 'carnegietsinghua.org']
    start_urls = [
        'https://carnegieendowment.org/publications/search-results?fltr=|&maxrow=18&tabName=books&channel=book&lang=en&pageOn=1']

    def parse(self, response):
        lis = response.xpath(
            "//li[@class='pub-list__pub col col-16 gutter-bottom--mobile']")
        for li in lis:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'en'
            item['organization'] = '卡内基国际和平基金会'
            item['target'] = 'carnegieendowment.org'
            item['project'] = 1
            url = li.xpath("./div/a/@href").extract_first()
            if url.startswith("/"):
                item['url'] = "https://carnegieendowment.org" + url
            else:
                item['url'] = url
            item['title'] = li.xpath("./div/strong/a/text()").extract_first()
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

        # 翻页
        next = response.xpath(
            "//a[contains(@class,'page-links__next')]/@href").extract_first()
        if next:
            if next.startswith('/'):
                next = "https://carnegieendowment.org" + next
                yield scrapy.Request(next, callback=self.parse)

    def detail_parser(self, response):
        item = response.meta.get('item')
        item['uuid'] = str(uuid.uuid1())
        publish_time = response.xpath(
            "//div[@class='meta pub-meta']/ul/li/text()").extract_first()
        if publish_time:
            item['publish_time'] = datetime.strptime(
                publish_time.strip(), "Published %B %d, %Y")
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
        item['init_time'] = datetime.now()
        author = response.xpath(
            "//div[@class='meta-heading']//text()").extract()
        author = "".join(author)
        author = author.replace("\n", "")
        author = author.replace("\xa0", "")
        item['author'] = author.replace(",", ";")
        keyword = response.xpath(
            "//div[contains(text(),'Related Topics')]/following-sibling::ul/li/a/text()").extract()
        if keyword:
            item['keyword'] = ";".join(keyword)
        abstract = response.xpath("//div[@class='article-body']/*")
        abstract_a = []
        count = 0
        for a in abstract:
            count += 1
            if a.extract().startswith('<p>'):
                wb = a.xpath(".//text()").extract()
                wb = "".join(wb)
                abstract_a.append(wb)
            elif not a.extract().startswith('<p>') and count > 3:
                break
            else:
                pass
        item['abstract'] = "\n".join(abstract_a)
        # item['abstract'] = "\n".join(abstract)
        item['content'] = item['abstract']
        file_link = response.xpath(
            "//a[@class='analytics-download']/@href").extract_first()
        if file_link:
            item['file_urls'] = [file_link]
        yield item


class AeiSpider(scrapy.Spider):
    name = 'aei'
    allowed_domains = ['aei.org']
    start_urls = ['https://www.aei.org/research-products/reports/']

    def parse(self, response):
        boxes = response.xpath("//div[@class='flex-col flex-1-3 news-item']")
        for box in boxes:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'en'
            item['organization'] = '美国企业公共政策研究所'
            item['target'] = 'www.aei.org'
            item['project'] = 1
            item['title'] = box.xpath(
                "./div[@class='news-teaser report-item']/h4[@class='entry-title']/a/text()").extract_first()
            publish_time = box.xpath(
                "./div[@class='news-teaser report-item']/div[@class='news-meta']/span/text()").extract_first()
            if publish_time:
                item['publish_time'] = datetime.strptime(
                    publish_time.strip(), "%B %d, %Y")
                item['year'] = datetime.strftime(item['publish_time'], "%Y")
            item['init_time'] = datetime.now()
            item['url'] = box.xpath(
                "./div[@class='news-teaser report-item']/h4[@class='entry-title']/a/@href").extract_first()
            author = box.xpath(
                "./div[@class='news-teaser report-item']/div[@class='news-authors']/a/text()").extract()
            item['author'] = ";".join(author)
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

        # 翻页
        next = response.xpath(
            "//a[contains(@class,'next')]/@href").extract_first()
        if next:
            yield scrapy.Request(next, callback=self.parse)

    def detail_parser(self, response):
        item = response.meta.get('item')
        item['uuid'] = str(uuid.uuid1())
        abstract = response.xpath(
            "//div[contains(@class,'the-content')]//text()").extract()
        abstract_a = []
        for a in [x.strip() for x in abstract]:
            if a:
                abstract_a.append(a)
        item['abstract'] = "\n".join(abstract_a)
        item['content'] = item['abstract']
        keyword = response.xpath(
            "//div[contains(@class,'tags')]//a/text()").extract()
        if keyword:
            item['keyword'] = ";".join(keyword)
        file_link = response.xpath(
            "//a[contains(@href,'.pdf')]/@href").extract_first()
        if file_link:
            item['file_urls'] = [file_link]
        else:
            file_link = response.xpath(
                "//iframe[@class='mobile-hide']/@src").extract_first()
            if file_link:
                item['file_urls'] = [file_link]
        yield item


class WHOSpider(scrapy.Spider):
    name = 'who'
    allowed_domains = ['who.int']
    start_urls = ['https://www.who.int/publications/list/all/zh/']

    def parse(self, response):
        links = response.xpath("//ul[@class='auto_archive']/li/a")
        for link in links:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'zh'
            item['organization'] = '世界卫生组织'
            item['target'] = 'www.who.int'
            item['project'] = 1
            item['title'] = link.xpath("./text()").extract_first()
            url = link.xpath("./@href").extract_first()
            item['url'] = "https://www.who.int" + url
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

    def detail_parser(self, response):
        item = response.meta.get('item')
        item['uuid'] = str(uuid.uuid1())
        item['author'] = response.xpath(
            "//strong[contains(text(),'作者')]/following-sibling::span/text()").extract_first()
        item['pages'] = response.xpath(
            "//strong[contains(text(),'页数')]/following-sibling::span[1]/text()").extract_first()
        year = response.xpath(
            "//strong[contains(text(),'出版日期')]/following-sibling::span[1]/text()").extract_first()
        year = re.findall(r"\d+", year)
        if year:
            item['year'] = year[0]
        if len(year) > 1:
            item['publish_time'] = datetime.strptime(
                "{}{}".format(year[0], year[1]), "%Y%m")
        elif len(year) == 1:
            item['publish_time'] = datetime.strptime(
                "{}".format(year[0]), "%Y")
        item['init_time'] = datetime.now()
        isbn = response.xpath(
            "//strong[contains(text(),'ISBN')]/following-sibling::span[1]/text()").extract_first()
        if isbn:
            item['journals_issns'] = 'ISBN:' + isbn
        file_link = response.xpath(
            "//h3[contains(text(),'下载')]/following-sibling::ul/li[1]/a/@href").extract_first()
        if file_link.startswith("/"):
            file_link = "https://www.who.int" + file_link
        if not file_link.endswith(".pdf"):
            file_link = re.findall(r".*\.pdf", file_link)[0]
        item['file_urls'] = [file_link]
        content = response.xpath(
            "//h3[contains(text(),'简介')]/following-sibling::p/text()").extract()
        item['content'] = '\n'.join([x.strip() for x in content])
        item['abstract'] = item['content']
        keyword = extract_tags(
            item['content'], topK=3, allowPOS=('n', 'v', 'vn'))
        item['keyword'] = ";".join(keyword)
        yield item


class CbdioSpider(scrapy.Spider):
    name = 'cbdio'
    allowed_domains = ['cbdio.com']
    start_urls = [
        "http://www.cbdio.com/node_2782.htm",
        "http://www.cbdio.com/node_2567.htm",
        "http://www.cbdio.com/node_2570.htm",
    ]

    def parse(self, response):
        lis = response.xpath("//div[@class='cb-media']/ul/li[@class='am-g']")
        for li in lis:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'zh'
            item['organization'] = '数据观'
            item['target'] = 'www.cbdio.com'
            item['project'] = 1
            item['title'] = li.xpath(
                "./div/p[@class='cb-media-title']/a/text()").extract_first()
            url = li.xpath(
                "./div/p[@class='cb-media-title']/a/@href").extract_first()
            if not url.startswith('http'):
                item['url'] = "http://www.cbdio.com/" + url
            else:
                item['url'] = url
            publish_time = li.xpath(
                "./div/p[@class='cb-media-datetime']/text()").extract_first()
            item['publish_time'] = datetime.strptime(
                publish_time, "%Y-%m-%d %H:%M")
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
            item['init_time'] = datetime.now()
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})
        next_page = response.xpath(
            "//i[@class='fa-angle-right']/../@href").extract_first()
        if next_page:
            next_page = "http://www.cbdio.com/" + next_page
            yield scrapy.Request(next_page, callback=self.parse)

    def detail_parser(self, response):
        item = response.meta.get('item')
        source = response.xpath(
            "//p[@class='cb-article-info']/span[1]/text()").extract_first()
        try:
            item['organization'] = re.findall(r"来源：(.*)", source)[0]
        except:
            item['organization'] = None
        author = response.xpath(
            "//p[@class='cb-article-info']/span[3]/text()").extract_first()
        try:
            item['author'] = re.findall(r"作者：(.*)", author)[0]
        except:
            item['author'] = None
        # abstract = response.xpath("//div[@class='cb-article']//strong[contains(text(),'以下为报告')]/../preceding-sibling::p[not(@class='cb-article-info')]/text()").extract()
        # item['abstract'] = "\n".join(abstract.reverse())
        # item['content'] = item['abstract']

        # file = response.xpath("//div[@class='cb-article']//strong[contains(text(),'以下为报告')]/following-sibling::a/@href").extract_first()
        # if file:
        #     item['file_urls'] = ["http://www.cbdio.com" + re.findall(r"\.\.\/\.\.\/\.\.(.*)",s)[0]]
        # else:
        #     pics = response.xpath("//div[@class='cb-article']//strong[contains(text(),'以下为报告')]/../following-sibling::p[@align='center'][position()<last()]/img/@src")
        abstract = response.xpath(
            "//div[@class='cb-article']/p[not(@class='cb-article-info')]/text()").extract()
        item['abstract'] = "".join(abstract)
        item['content'] = item['abstract']
        keyword = extract_tags(
            item['abstract'], topK=2, allowPOS=('n', 'v', 'vn'))
        item['keyword'] = ";".join(keyword)
        file = response.xpath(
            "//div[@class='cb-article']//strong[contains(text(),'以下为报告')]/following-sibling::a/@href").extract_first()
        if file:
            try:
                item['file_urls'] = ["http://www.cbdio.com" +
                                     re.findall(r"\.\.\/\.\.\/\.\.(.*)", file)[0]]
            except:
                item['file_urls'] = [file]
        else:
            pics = response.xpath("//p[@align='center']/img/@src").extract()
            if pics:
                item['file_urls'] = ["http://www.cbdio.com" +
                                     re.findall(r"\.\.\/\.\.\/\.\.(.*)", x)[0] for x in pics]
        item['uuid'] = str(uuid.uuid1())
        yield item


def isPdf(fileurl):
    if fileurl.strip().lower().endswith('pdf'):
        return True


class UnidoSpider(scrapy.Spider):
    name = "unido"
    allowed_domains = ['unido.org']
    start_urls = ['https://www.unido.org/researchers/publications']

    def parse(self, response):
        links = response.xpath(
            "//a[@class='btn btn--primary']/@href").extract()
        for link in links:
            link = "https://www.unido.org" + link
            yield scrapy.Request(link.strip(), callback=self.page_parse)

    def page_parse(self, response):
        divs = response.xpath("//div[@class='col-xs-12 col-sm-9']")
        if divs:
            for div in divs:
                a_s = div.xpath(".//a")
                if len(a_s) > 1:
                    a_links = div.xpath(".//a/@href").extract()
                    if all(map(isPdf, a_links)):
                        # 多个按钮全部都是pdf
                        chinese_link = div.xpath(
                            ".//a[contains(text(),'Chinese')]/@href").extract_first()
                        if chinese_link:
                            file_link = chinese_link
                        else:
                            english_link = div.xpath(
                                ".//a[contains(text(),'English')]/@href").extract_first()
                            if english_link:
                                file_link = english_link
                            else:
                                file_link = a_links[0]
                        item = ReportItem()
                        item['crawler_ip'] = get_host_ip()
                        item['category'] = 'report'
                        item['language'] = 'en'
                        item['organization'] = '联合国工业发展组织'
                        item['target'] = 'www.unido.org'
                        item['url'] = response.url
                        item['uuid'] = str(uuid.uuid1())
                        item['project'] = 1
                        item['author'] = None
                        item['content'] = None
                        item['abstract'] = None
                        item['publish_time'] = None
                        item['init_time'] = datetime.now()
                        item['title'] = div.xpath(
                            ".//p/strong/text()").extract_first()
                        if not file_link.startswith('http'):
                            item['file_urls'] = [
                                'https://www.unido.org' + file_link.strip()]
                        else:
                            item['file_urls'] = [file_link.strip()]
                        yield item

                    else:
                        for link in a_links:
                            if link.lower().endswith("pdf"):
                                item = ReportItem()
                                item['crawler_ip'] = get_host_ip()
                                item['category'] = 'report'
                                item['language'] = 'en'
                                item['organization'] = '联合国工业发展组织'
                                item['target'] = 'www.unido.org'
                                item['url'] = response.url
                                item['author'] = None
                                item['content'] = None
                                item['abstract'] = None
                                item['publish_time'] = None
                                item['uuid'] = str(uuid.uuid1())
                                item['project'] = 1
                                item['init_time'] = datetime.now()
                                item['title'] = div.xpath(
                                    ".//p/strong/text()").extract_first()
                                if not link.startswith('http'):
                                    item['file_urls'] = [
                                        'https://www.unido.org' + link.strip()]
                                else:
                                    item['file_urls'] = [link.strip()]
                                yield item

                            else:
                                if not link.startswith('http'):
                                    link = 'https://www.unido.org' + link
                                yield scrapy.Request(link.strip(), callback=self.page_parse)
                else:
                    # 只有一个按钮
                    link = a_s[0].xpath("./@href").extract_first()
                    if isPdf(link):
                        item = ReportItem()
                        item['crawler_ip'] = get_host_ip()
                        item['category'] = 'report'
                        item['language'] = 'en'
                        item['organization'] = '联合国工业发展组织'
                        item['target'] = 'www.unido.org'
                        item['url'] = response.url
                        item['author'] = None
                        item['content'] = None
                        item['abstract'] = None
                        item['publish_time'] = None
                        item['uuid'] = str(uuid.uuid1())
                        item['project'] = 1
                        item['init_time'] = datetime.now()
                        item['title'] = div.xpath(
                            ".//p/strong/text()").extract_first()
                        if not link.startswith('http'):
                            item['file_urls'] = [
                                'https://www.unido.org' + link.strip()]
                        else:
                            item['file_urls'] = [link.strip()]
                        yield item

                    else:
                        if not link.startswith('http'):
                            link = 'https://www.unido.org' + link
                        yield scrapy.Request(link.strip(), callback=self.page_parse)

        else:
            # 已到末尾页
            divs_1 = response.xpath(
                "//div[@class='image-inline']/following-sibling::div")
            if divs_1:
                for div in divs_1:
                    item = ReportItem()
                    item['crawler_ip'] = get_host_ip()
                    item['category'] = 'report'
                    item['language'] = 'en'
                    item['organization'] = '联合国工业发展组织'
                    item['target'] = 'www.unido.org'
                    item['url'] = response.url
                    item['author'] = None
                    item['content'] = None
                    item['abstract'] = None
                    item['publish_time'] = None
                    item['uuid'] = str(uuid.uuid1())
                    item['project'] = 1
                    item['init_time'] = datetime.now()
                    item['title'] = div.xpath(
                        ".//p/strong/text()").extract_first()
                    detail = div.xpath(".//span/span/p/text()").extract_first()
                    detail_re = re.findall(r"(\d+) \((\d+) .*", detail)
                    if detail_re:
                        item['year'] = detail_re[0][0]
                        item['pages'] = int(detail_re[0][1])
                    file_link = div.xpath(
                        ".//ul/li[1]/a/@href").extract_first()
                    if not file_link.startswith('http'):
                        item['file_urls'] = [
                            'https://www.unido.org' + file_link.strip()]
                    else:
                        item['file_urls'] = [file_link.strip()]
                    yield item

            else:
                article_body = response.xpath(
                    "//div[@class='content article__body']")
                if article_body:
                    item = ReportItem()
                    item['crawler_ip'] = get_host_ip()
                    item['category'] = 'report'
                    item['language'] = 'en'
                    item['organization'] = '联合国工业发展组织'
                    item['target'] = 'www.unido.org'
                    item['project'] = 1
                    item['author'] = None
                    item['content'] = None
                    item['abstract'] = None
                    item['publish_time'] = None
                    item['url'] = response.url
                    item['uuid'] = str(uuid.uuid1())
                    item['init_time'] = datetime.now()
                    item['title'] = article_body.xpath(
                        ".//h2/text()").extract_first()
                    item['abstract'] = article_body.xpath(
                        ".//h3[contains(text(),'Abstract')]/following-sibling::p[1]//text()").extract_first()
                    details = article_body.xpath(
                        ".//h3[contains(text(),'Other Information')]/following-sibling::p//text()").extract()
                    for detail in details:
                        if detail.startswith('Date'):
                            item['year'] = re.findall(r"\d+", detail)[0]
                    file_link = article_body.xpath(
                        ".//a[contains(text(),'Download Full')]/@href").extract_first()
                    if file_link:
                        if not file_link.startswith('http'):
                            item['file_urls'] = [
                                'https://www.unido.org' + file_link.strip()]
                        else:
                            item['file_urls'] = [file_link.strip()]
                    yield item


class CngascnSpider(scrapy.Spider):
    name = 'cngascn'
    allowed_domains = ['cngascn.com']
    start_urls = [
        'http://www.cngascn.com/report/',
        'http://www.cngascn.com/stateLaws/'
    ]

    def parse(self, response):
        if 'report' in response.url:
            lis = response.xpath("//ul[@class='ul4']/li")
            for li in lis:
                detail_link = li.xpath(".//a/@href").extract_first()
                if not detail_link.startswith('http'):
                    detail_link = 'http://www.cngascn.com' + detail_link
                yield scrapy.Request(detail_link, callback=self.report_parse)
            next_page = response.xpath(
                "//a[contains(text(),'下一页')]/@href").extract_first()
            if not next_page.startswith('http'):
                next_page = 'http://www.cngascn.com' + next_page
            if next_page != response.url:
                yield scrapy.Request(next_page, callback=self.parse)
        else:
            lis = response.xpath("//ul[@class='ul4']/li")
            for li in lis:
                detail_link = li.xpath(".//a/@href").extract_first()
                if not detail_link.startswith('http'):
                    detail_link = 'http://www.cngascn.com' + detail_link
                yield scrapy.Request(detail_link, callback=self.news_parse)
            next_page = response.xpath(
                "//a[contains(text(),'下一页')]/@href").extract_first()
            if not next_page.startswith('http'):
                next_page = 'http://www.cngascn.com' + next_page
            if next_page != response.url:
                yield scrapy.Request(next_page, callback=self.parse)

    def report_parse(self, response):
        item = ReportItem()
        item['crawler_ip'] = get_host_ip()
        item['category'] = 'report'
        item['language'] = 'zh'
        item['organization'] = '天然气工业'
        item['target'] = 'www.cngascn.com'
        item['project'] = 1
        item['url'] = response.url
        item['title'] = response.xpath("//h3/text()").extract_first()
        source_and_time = response.xpath(
            "//span[@class='time']/text()").extract_first()
        publish_time = re.findall(r'日期：(.*?) ', source_and_time)
        if publish_time:
            publish_time = publish_time[0]
            item['publish_time'] = datetime.strptime(publish_time, '%Y-%m-%d')
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
        else:
            item['publish_time'] = None
            item['year'] = None
        item['init_time'] = datetime.now()
        item['abstract'] = response.xpath(
            "string(//div[@id='speci'])").extract_first().strip()
        item['content'] = item['abstract']
        keyword = extract_tags(
            item['abstract'], topK=2, allowPOS=('n', 'v', 'vn'))
        item['keyword'] = ";".join(keyword)
        author = re.findall(r'来源：(.*?) ', source_and_time)
        if author:
            item['author'] = author[0]
        file = response.xpath(
            "//a[@class='ke-insertfile']/@href").extract_first()
        if file:
            item['file_urls'] = [file]
        item['uuid'] = str(uuid.uuid1())
        yield item

    def news_parse(self, response):
        item = NewsItem()
        item['crawler_ip'] = get_host_ip()
        item['category'] = 'news'
        item['language'] = 'zh'
        item['target'] = '天然气工业'
        item['domain'] = 'www.cngascn.com'
        item['project'] = 1
        item['url'] = response.url
        item['title'] = response.xpath("//h3/text()").extract_first()
        source_and_time = response.xpath(
            "//span[@class='time']/text()").extract_first()
        publish_time = re.findall(r'日期：(.*?) ', source_and_time)
        if publish_time:
            publish_time = publish_time[0]
            item['publish_time'] = datetime.strptime(publish_time, '%Y-%m-%d')
        item['init_time'] = datetime.now()
        source = re.findall(r'来源：(.*?) ', source_and_time)
        if source:
            item['source'] = source[0]
        item['abstract'] = None
        item['content'] = response.xpath(
            "string(//div[@id='speci'])").extract_first().strip()
        keyword = extract_tags(
            item['content'], topK=2, allowPOS=('n', 'v', 'vn'))
        item['keyword'] = ";".join(keyword)
        item['content_html'] = response.xpath(
            "//div[@id='speci']").extract_first()
        item['uuid'] = str(uuid.uuid1())
        yield item


class EiaSpider(scrapy.Spider):
    name = 'eia'
    allowed_domains = ['eia.gov']
    start_urls = [
        'https://www.eia.gov/global/includes/bookshelf/index.php?tags=128',
        'https://www.eia.gov/global/includes/bookshelf/index.php?tags=132',
        'https://www.eia.gov/global/includes/bookshelf/index.php?tags=1139'
    ]

    def parse(self, response):
        response_d = json.loads(response.text)
        for report in response_d['reports']:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'en'
            item['organization'] = '美国能源信息管理局'
            item['target'] = 'www.eia.gov'
            item['project'] = 1
            item['title'] = report['title']
            item['abstract'] = report['summary_descript']
            item['content'] = item['abstract']
            publish_time = re.findall(
                r"(^\S+)\s+(\d+), (\d+)", report['release_date'])
            item['year'] = publish_time[0][2]
            publish_time = "-".join(publish_time[0])
            item['publish_time'] = datetime.strptime(publish_time, "%B-%d-%Y")
            item['init_time'] = datetime.now()
            final_tag_list = []
            tags = report['alltags']
            tags = tags.split(',')
            for tag in tags:
                for tag_d in response_d['tags']:
                    target_tag = 'T' + tag
                    if tag_d['identifier'] == target_tag:
                        final_tag_list.append(tag_d['label'])
            item['tags'] = ';'.join(final_tag_list)
            item['keyword'] = item['tags']
            url = report['link_html']
            if not url.startswith("http"):
                url = 'https://www.eia.gov' + url
            if 'cfm#' in url:
                url = url.split('#')[0]
            if ' ' in url:
                url = url.replace(' ', '')
            item['url'] = url
            yield scrapy.Request(item['url'], callback=self.detail_parse, meta={'item': item.copy()})

    def detail_parse(self, response):
        item = response.meta.get('item')
        file1 = response.xpath("//a[@class='ico_pdf']/@href").extract_first()
        file_link = None
        if file1:
            if not file1.startswith('/'):
                base_list = response.url.split('/')
                base_list[-1] = file1
                file_link = '/'.join(base_list)
            else:
                file_link = 'https://www.eia.gov' + file1

        file2 = response.xpath(
            "//span[@class='report_summary']/a/@href").extract_first()
        if file2:
            file_link = response.url + file2

        file3 = response.xpath(
            "//a[contains(text(),'friendly version')]/@href").extract_first()
        if file3:
            file_link = response.url + file3

        file7 = response.xpath(
            "//td[contains(text(),'friendly version')]/following-sibling::td//a/@href").extract_first()
        if file7:
            file_link = response.url + file7

        file8 = response.xpath(
            "//a[contains(text(),'{}')]/@href".format(item['title'])).extract_first()
        if file8 and file8 != '#':
            file_link = response.url + file8

        file4 = response.xpath("//a[@class='ico pdf']/@href").extract_first()
        if file4:
            if not file4.startswith('/'):
                base_list = response.url.split('/')
                base_list[-1] = file4
                file_link = '/'.join(base_list)
            else:
                file_link = 'https://www.eia.gov' + file4

        file5 = response.xpath("//a[@class='pdf']/@href").extract_first()
        if file5:
            if not file5.startswith('/'):
                base_list = response.url.split('/')
                base_list[-1] = file5
                file_link = '/'.join(base_list)
            else:
                file_link = 'https://www.eia.gov' + file5

        file6 = response.xpath(
            "//li[@class='ico pdf']/a/@href").extract_first()
        if file6:
            if not file6.startswith('/'):
                base_list = response.url.split('/')
                base_list[-1] = file6
                file_link = '/'.join(base_list)
            else:
                file_link = 'https://www.eia.gov' + file6
        item['author'] = None
        item['uuid'] = str(uuid.uuid1())
        if file_link:
            item['file_urls'] = [file_link]
            yield item

        # //a[@class='pdf']/@href
        # //a[@class='ico_pdf']/@href
        # 分析所有url构成 php结尾一般有文件 /结尾有文件 但拼接方式不同


class CkcestSpider(scrapy.Spider):
    name = 'ckcest'
    allowed_domains = ['ckcest.cn']

    def start_requests(self):
        data = {
            'searchType': 'SciTechReport',
            'before_searchText': '',
            'facetFilter': 'restypeFacet@234@@345@美国农业部报告@345@',
            'topnum': '20',
            'facetField': 'restypeFacet',
            'index': '1',
            'open': 'true'
        }
        yield scrapy.FormRequest('http://agri.ckcest.cn/senior/searchFacet.html', formdata=data, callback=self.parse)

    def parse(self, response):
        json_d = json.loads(response.text)
        rows = json_d['rows']['美国农业部报告']
        data = {
            'searchType': 'SciTechReport',
            'before_searchText': '',
            'facetFilter': 'restypeFacet@234@@345@美国农业部报告@345@',
            'order_by': 'year',
            'start': '1',
            'end': str(rows),
            'journal': 'false'
        }
        yield scrapy.FormRequest('http://agri.ckcest.cn/senior/searchList.html', formdata=data, callback=self.parse_rows)

    def parse_rows(self, response):
        json_d = json.loads(response.text)
        reports = json_d['data']
        for report in reports:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'en'
            item['organization'] = '美国农业部'
            item['target'] = 'agri.ckcest.cn'
            item['project'] = 1
            url = "http://agri.ckcest.cn/specialtyresources/industryreport/detail/{}.html".format(
                report['gid'])
            item['title'] = report['zhongwenmingcheng']
            item['abstract'] = report['zhongwenzhaiyao']
            item['content'] = item['abstract']
            keyword = report['guanjianzi']
            keyword = keyword.split('`')
            item['keyword'] = ';'.join(keyword)
            item['init_time'] = datetime.now()
            publish_time = report['riqi']
            if '/' in publish_time:
                try:
                    item['publish_time'] = datetime.strptime(
                        publish_time, '%Y/%m/%d')
                    item['year'] = datetime.strftime(
                        item['publish_time'], "%Y")
                except:
                    item['publish_time'] = datetime.strptime(
                        publish_time, '%Y/%m')
                    item['year'] = datetime.strftime(
                        item['publish_time'], "%Y")
            else:
                item['year'] = publish_time
                item['publish_time'] = datetime.strptime(
                    "{}/1/1".format(publish_time), '%Y/%m/%d')
            author = report['zuozhe']
            if '`' in author:
                author = author.split('`')
                item['author'] = ';'.join(author)
            else:
                item['author'] = author
            file = report['wenjian']
            if file:
                file_link = "http://agri.ckcest.cn/" + file
            item['file_urls'] = [file_link]
            item['uuid'] = str(uuid.uuid1())
            yield scrapy.Request(url, callback=self.url_parse, meta={'item': item.copy()})

    def url_parse(self, response):
        item = response.meta.get('item')
        url = response.xpath(
            "//dt[text()='来源地址：']/following-sibling::dd/a/@href").extract_first()
        item['url'] = url
        yield item


class CnasSpider(scrapy.Spider):
    name = 'cnas'
    allowed_domains = ['cnas.org', 'amazonaws.com']
    start_urls = [
        'https://www.cnas.org/reports?area=technology-and-national-security',
        'https://www.cnas.org/reports?area=energy-economics-and-security'
    ]

    def parse(self, response):
        lis = response.xpath("//ul[@class='entry-listing']/li")
        for li in lis:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'en'
            item['organization'] = '新美国安全中心'
            item['target'] = 'www.cnas.org'
            item['project'] = 1
            item['title'] = li.xpath("./a/text()").extract_first()
            publish_time = li.xpath("./ul/li/text()").extract_first()
            item['publish_time'] = datetime.strptime(publish_time, '%B %d, %Y')
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
            item['init_time'] = datetime.now()
            item['url'] = li.xpath("./a/@href").extract_first()
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})
        # 翻页
        next_page = response.xpath(
            "//div[@class='pagination']//span[contains(@class,'-right')]/../@href").extract_first()
        if next_page:
            yield scrapy.Request(next_page, callback=self.parse)

    def detail_parser(self, response):
        item = response.meta.get('item')
        item['subtitle'] = response.xpath(
            "//p[contains(@class,'subtitle')]/text()").extract_first()
        author = response.xpath("//a[@class='contributor']/text()").extract()
        item['author'] = ';'.join(author)
        content = response.xpath(
            "string(//div[contains(@id,'mainbar')])").extract_first()
        item['content'] = content.strip()
        item['abstract'] = item['content']
        keywords = rake.run(item['abstract'])
        keywords = [tuple[0] for tuple in keywords[:2]]
        item['keyword'] = ';'.join(keywords)
        file = response.xpath(
            "//a[@download and contains(text(),'PDF')]/@href").extract_first()
        if file:
            file_link = 'https:' + file.split('?')[0]
        else:
            file_link = None
        item['file_urls'] = [file_link]
        item['uuid'] = str(uuid.uuid1())
        yield item


class CommerceSpider(scrapy.Spider):
    name = 'commerce'
    allowed_domains = ['commerce.gov']
    start_urls = ['https://www.commerce.gov/data-and-reports/reports']

    def parse(self, response):
        trs = response.xpath("//div[@class='view-content']/table/tbody/tr")
        for tr in trs:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'en'
            item['organization'] = '美国商务部'
            item['target'] = 'www.commerce.gov'
            item['project'] = 1
            item['title'] = tr.xpath(
                "./td[contains(@class,'views-field-title')]/a/text()").extract_first()
            url = tr.xpath(
                "./td[contains(@class,'views-field-title')]/a/@href").extract_first()
            if not url.startswith('http'):
                url = "https://www.commerce.gov" + url
            item['url'] = url
            files = tr.xpath(".//ul//a/@href").extract()
            item['file_urls'] = files
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})
        next_page = response.xpath(
            "//a[@title='Go to next page']/@href").extract_first()
        if next_page:
            if not next_page.startswith('http'):
                next_page = "https://www.commerce.gov/data-and-reports/reports" + next_page
            yield scrapy.Request(next_page, callback=self.parse)

    def detail_parser(self, response):
        item = response.meta.get('item')
        publish_time = response.xpath(
            "//time[@datetime]/@datetime").extract_first()
        item['publish_time'] = datetime.strptime(
            publish_time, "%Y-%m-%dT%H:%M:%SZ")
        item['year'] = datetime.strftime(item['publish_time'], "%Y")
        item['init_time'] = datetime.now()
        item['author'] = response.xpath(
            "//release-infobox//h2/a/div/text()").extract_first()
        item['abstract'] = response.xpath(
            "string(//release-infobox/following-sibling::div)").extract_first()
        item['content'] = item['abstract']
        keywords = rake.run(item['abstract'])
        keywords = [tuple[0] for tuple in keywords[:2]]
        item['keyword'] = ';'.join(keywords)
        item['uuid'] = str(uuid.uuid1())
        yield item


class ZkyGcyNersSpider(scrapy.Spider):
    name = "zkygcy"
    allowed_domains = ['cas.cn', 'cae.cn']
    start_urls = [
        'http://www.cae.cn/cae/html/main/col84/column_84_1.html',
        'http://www.cas.cn/syky/',
        'http://www.cas.cn/zjs/index.shtml',
        'http://www.cas.cn/zt/sszt/kjgzbd/zjsd/',
    ]

    def parse(self, response):
        if 'www.cae.cn' in response.url:
            lis = response.xpath("//div[@class='right_md_list']//li")
            for li in lis:
                item = NewsItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'news'
                item['language'] = 'zh'
                item['domain'] = 'www.cae.cn'
                item['target'] = '中国工程院'
                item['project'] = 1
                publish_time = li.xpath("./span/text()").extract_first()
                item['publish_time'] = datetime.strptime(
                    publish_time, "%Y-%m-%d")
                item['init_time'] = datetime.now()
                item['title'] = li.xpath("./a/text()").extract_first()
                url = li.xpath("./a/@href").extract_first()
                if not url.startswith("http"):
                    url = "http://www.cae.cn" + url
                item['url'] = url
                yield scrapy.Request(item['url'], callback=self.cae_detail, meta={'item': item.copy()})

            next_page = response.xpath(
                "//a[contains(text(),'下一页')]/@href").extract_first()
            if next_page:
                if not next_page.startswith('http'):
                    next_page = "http://www.cae.cn" + next_page
                yield scrapy.Request(next_page, callback=self.parse)

        elif 'www.cas.cn' in response.url:
            flip = response.meta.get('flip')
            if 'kjgzbd' in response.url:
                a_s = response.xpath("//table[3]//a")
                for a in a_s:
                    item = NewsItem()
                    item['crawler_ip'] = get_host_ip()
                    item['category'] = 'news'
                    item['language'] = 'zh'
                    item['domain'] = 'www.cas.cn'
                    item['target'] = '中国科学院'
                    item['project'] = 1
                    item['uuid'] = str(uuid.uuid1())
                    item['area'] = '抗击冠状病毒'
                    url = a.xpath("./@href").extract_first()
                    if url.startswith('./'):
                        item['url'] = "http://www.cas.cn/zt/sszt/kjgzbd/zjsd" + url[1:]
                    else:
                        continue
                    item['title'] = a.xpath("./@title").extract_first()
                    yield scrapy.Request(item['url'], callback=self.gzbd_detail_parser, meta={'item': item.copy()})
                # 翻页
                if not flip:
                    countPage = int(re.findall(
                        r"var countPage = (\d+)", response.text)[0])
                    if countPage > 1:
                        for c in range(1, countPage):
                            next_page = "http://www.cas.cn/zt/sszt/kjgzbd/zjsd/index_{}.shtml".format(
                                c)
                            yield scrapy.Request(next_page, callback=self.parse, meta={'flip': 1})

            else:
                lis = response.xpath(
                    "//div[@id='content']/li[not(contains(@class,'gl_line'))]")
                for li in lis:
                    item = NewsItem()
                    item['crawler_ip'] = get_host_ip()
                    item['category'] = 'news'
                    item['language'] = 'zh'
                    item['domain'] = 'www.cas.cn'
                    item['target'] = '中国科学院'
                    item['project'] = 1
                    publish_time = li.xpath(
                        "./span/text()").extract_first().strip()
                    item['publish_time'] = datetime.strptime(
                        publish_time, "%Y/%m/%d")
                    item['init_time'] = datetime.now()
                    item['title'] = li.xpath("./a/@title").extract_first()
                    url = li.xpath("./a/@href").extract_first()
                    if url.startswith('./'):
                        if 'syky' in response.url:
                            url = "http://www.cas.cn/syky" + url[1:]
                        else:
                            url = "http://www.cas.cn/zjs" + url[1:]
                    item['url'] = url
                    yield scrapy.Request(item['url'], callback=self.cas_detail, meta={'item': item.copy()})

                if not flip:
                    countPage = int(re.findall(
                        r"var countPage = (\d+)", response.text)[0])
                    if countPage > 1:
                        for c in range(1, countPage):
                            if 'syky' in response.url:
                                next_page = "http://www.cas.cn/syky/index_{}.shtml".format(
                                    c)
                            else:
                                next_page = "http://www.cas.cn/zjs/index_{}.shtml".format(
                                    c)
                            yield scrapy.Request(next_page, callback=self.parse, meta={'flip': 1})

    def cae_detail(self, response):
        item = response.meta.get('item')
        source = response.xpath(
            "//div[@class='right_md_laiy']/h4/text()").extract_first()
        item['source'] = source.strip()
        item['content_html'] = response.xpath(
            "//div[@id='zoom']").extract_first()
        item['content'] = response.xpath(
            "string(//div[@id='zoom'])").extract_first().strip()
        item['abstract'] = item['content']
        keyword = extract_tags(
            item['content'], topK=2, allowPOS=('n', 'v', 'vn'))
        item['keyword'] = ";".join(keyword)
        item['author'] = None
        item['uuid'] = str(uuid.uuid1())
        yield item

    def cas_detail(self, response):
        item = response.meta.get('item')
        source = re.findall(
            r"\<\!\-\-文章来源\-\-\>(.*)\<\!\-\-文章来源\-\-\>", response.text)
        if source:
            item['source'] = source[0]
        content_html = response.xpath(
            "//div[@class='TRS_Editor']//div[@class='TRS_Editor']/*[not(name()='style')]").extract()
        if not content_html:
            content_html = response.xpath(
                "//div[@class='TRS_Editor']/*[not(name()='style')]").extract()
        item['content_html'] = "\n".join(content_html)
        html = etree.HTML(item['content_html'])
        try:
            item['content'] = html.xpath("string(.)").strip()
        except:
            return
        item['abstract'] = item['content']
        keyword = extract_tags(
            item['content'], topK=2, allowPOS=('n', 'v', 'vn'))
        item['keyword'] = ";".join(keyword)
        item['author'] = None
        item['uuid'] = str(uuid.uuid1())
        yield item
        # print(item)

    def gzbd_detail_parser(self, response):
        item = response.meta.get('item')
        publish_time = response.xpath(
            "//td[contains(text(),'发布时间')]/text()").extract_first()
        print(publish_time)
        try:
            publish_time = re.findall(r"(\d+\-\d+\-\d+)", publish_time)[0]
            item['publish_time'] = datetime.strptime(publish_time, "%Y-%m-%d")
        except:
            item['publish_time'] = None
        item['init_time'] = datetime.now()
        source = response.xpath(
            "//td[contains(text(),'来源')]/text()").extract_first()
        try:
            item['source'] = re.findall(r"来源：(.*)", source)[0]
        except:
            item['source'] = None
        abstract = response.xpath(
            "string(//div[@class='TRS_Editor']//div[@class='TRS_Editor']/div)").extract_first()
        if abstract:
            item['abstract'] = abstract.strip()
            item['content'] = item['abstract']
            item['content_html'] = response.xpath(
                "//div[@class='TRS_Editor']//div[@class='TRS_Editor']/div").extract_first()
        else:
            abstract = response.xpath(
                "string(//div[@class='TRS_Editor'])").extract_first()
            abstract = re.findall(r"\.TRS.*}(.*)", abstract, re.S)
            if abstract:
                item['abstract'] = abstract[0].strip()
                item['content'] = item['abstract']
                item['content_html'] = response.xpath(
                    "//div[@class='TRS_Editor']").extract_first()
        keyword = extract_tags(
            item['content'], topK=2, allowPOS=('n', 'v', 'vn'))
        item['keyword'] = ";".join(keyword)
        item['author'] = None
        yield item
        # print(item)


class RandSpider(scrapy.Spider):
    name = 'rand'
    allowed_domains = ['rand.org']
    start_urls = [
        'https://www.rand.org/topics/cyber-and-data-sciences.html',
        'https://www.rand.org/topics/infrastructure-and-transportation.html',
        'https://www.rand.org/topics/science-and-technology.html',
        'https://www.rand.org/topics/artificial-intelligence.html',
        'https://www.rand.org/topics/global-climate-change.html',
        'https://www.rand.org/topics/coronavirus-disease-2019-covid-19.html'
    ]

    def parse(self, response):
        lis = response.xpath("//ul[@class='teasers list organic']/li")
        for li in lis:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'en'
            item['organization'] = '兰德公司'
            item['target'] = 'www.rand.org'
            item['project'] = 1
            publish_time = li.xpath(
                "./div[@class='text']/div[@class='flex-wrap']/p/text()").extract_first()
            if publish_time:
                item['publish_time'] = datetime.strptime(
                    publish_time, "%b %d, %Y")
            else:
                item['publish_time'] = None
            item['init_time'] = datetime.now()
            link = li.xpath("./div[@class='text']/h3/a/@href").extract_first()
            yield scrapy.Request(link, callback=self.detail_parser, meta={'item': item.copy()})
        next_page = response.xpath(
            "//li[@class='next']/a/@href").extract_first()
        if next_page:
            next_page = "https://www.rand.org" + next_page
            yield scrapy.Request(next_page, callback=self.parse)
    def detail_parser(self, response):
        item = response.meta.get('item')
        file = response.xpath(
            "//table[@class='ebook']//tr//span[@class='format-pdf']/a[@class='btn']/@href").extract_first()
        if file:
            item['url'] = response.url
            item['title'] = response.xpath(
                "//h1[@id='RANDTitleHeadingId']/text()").extract_first()
            author = response.xpath(
                "//div[contains(@class,'header')]//p[@class='authors']/a/text()").extract()
            item['author'] = ";".join(author)
            keyword = response.xpath(
                "//ul[@class='related-topics']/li/a/text()").extract()
            item['keyword'] = ";".join(keyword)
            pages = response.xpath(
                "//aside[@class='document-details']//strong[contains(text(),'Pages')]/../text()").extract_first()
            if pages:
                item['pages'] = pages.strip()
            year = response.xpath(
                "//aside[@class='document-details']//strong[contains(text(),'Year')]/../text()").extract_first()
            if year:
                item['year'] = year.strip()
            abstract = response.xpath(
                "string(//div[@class='abstract product-page-abstract'])").extract_first()
            if abstract:
                item['abstract'] = abstract.strip()
                item['content'] = item['abstract']
            else:
                item['abstract'], item['content'] = None, None
            item['uuid'] = str(uuid.uuid1())
            file = "https://www.rand.org" + file
            item['file_urls'] = [file]
            yield item


class CeriSpider(scrapy.Spider):
    name = 'ceri'
    allowed_domains = ['ceri.ca']
    start_urls = ['https://ceri.ca/studies/']

    def parse(self, response):
        links1 = response.xpath(
            "//div[@class='row' and position()>1]/div[@class='col-sm-6']/a[1]/@href").extract()
        links2 = response.xpath(
            "//h4[@class='year-toggle']/following-sibling::div/a/@href").extract()
        links = links1 + links2
        for link in links:
            link = "https://ceri.ca" + link
            yield scrapy.Request(link.strip(), callback=self.detail_parse)

    def detail_parse(self, response):
        item = ReportItem()
        item['url'] = response.url
        item['crawler_ip'] = get_host_ip()
        item['category'] = 'report'
        item['language'] = 'en'
        item['organization'] = '加拿大能源研究机构CERI'
        item['target'] = 'ceri.ca'
        item['project'] = 1
        item['title'] = response.xpath(
            "//div[@class='page-sub-container']/h2/text()").extract_first()
        publish_time = response.xpath(
            "//div[@class='page-sub-container']/p[contains(text(),'Published')]/text()").extract_first()
        item['publish_time'] = datetime.strptime(
            publish_time, "Published On: %B %d, %Y")
        item['year'] = datetime.strftime(item['publish_time'], "%Y")
        item['init_time'] = datetime.now()
        abstract = response.xpath(
            "//div[@class='page-sub-container']/div[@class='sharethis-inline-share-buttons margin-vertical-2rem']/following-sibling::p/text()").extract()
        item['abstract'] = "\n".join(abstract)
        item['content'] = item['abstract']
        keywords = rake.run(item['abstract'])
        keywords = [tuple[0] for tuple in keywords[:2]]
        item['keyword'] = ';'.join(keywords)
        item['author'] = None
        item['uuid'] = str(uuid.uuid1())
        file = response.xpath(
            "//a[contains(text(),'Full')]/@href").extract_first()
        if file:
            item['file_urls'] = ["https://ceri.ca" + file]
        yield item


class AtseSpider(scrapy.Spider):
    name = 'atse'
    allowed_domains = ['atse.org.au']
    start_urls = ['https://www.atse.org.au/research-and-policy/publications/']

    def parse(self, response):
        a_s = response.xpath(
            "//div[@class='columns is-multiline js-filterable_cards']/div/a")
        for a in a_s:
            url = a.xpath("./@href").extract_first()
            yield scrapy.Request(url, callback=self.detail_parse)
        next_page = response.xpath(
            "//a[@class='pagination-next']/@href").extract_first()
        if next_page:
            yield scrapy.Request(next_page, callback=self.parse)

    def detail_parse(self, response):
        item = ReportItem()
        item['url'] = response.url
        item['crawler_ip'] = get_host_ip()
        item['category'] = 'report'
        item['language'] = 'en'
        item['organization'] = '澳大利亚技术科学与工程院'
        item['target'] = 'www.atse.org.au'
        item['project'] = 1
        item['title'] = response.xpath(
            "//h1[contains(@class,'title')]/text()").extract_first()
        try:
            item['abstract'] = response.xpath(
                "string(//div[@class='mod_richtext-content content'])").extract_first().strip()
        except:
            item['abstract'] = None
        item['content'] = item['abstract']
        keyword = response.xpath(
            "//div[@class='mod_footer-tags']/p/text()").extract_first()
        if keyword:
            item['keyword'] = keyword.replace(',', ';').strip()
        publish_time = response.xpath(
            "//section[@class='side_publication']//div[@class='side_publication-text']/p[@class='is-meta']/text()").extract_first()
        publish_time = publish_time.replace('Published', '01')
        item['publish_time'] = datetime.strptime(publish_time, '%d %B %Y')
        item['year'] = datetime.strftime(item['publish_time'], "%Y")
        item['init_time'] = datetime.now()
        item['uuid'] = str(uuid.uuid1())
        file_link = response.xpath(
            "//section[@class='side_publication']//div[@class='side_publication-text']/a/@href").extract_first()
        item['file_urls'] = [file_link]
        yield item


class MEESpider(scrapy.Spider):
    name = 'MEE'
    allowed_domains = ['mee.gov.cn']

    def start_requests(self):
        start_dict = {
            'http://www.mee.gov.cn/hjzl/sthjzk/jagb/': 'report',
            'http://www.mee.gov.cn/hjzl/sthjzk/sthjtjnb/': 'report',
            'http://www.mee.gov.cn/hjzl/sthjzk/jagb/': 'report',
            'http://www.mee.gov.cn/hjzl/sthjzk/hjzywr/': 'report',
            'http://www.mee.gov.cn/hjzl/sthjzk/gtfwwrfz/': 'report',
            'http://www.mee.gov.cn/hjzl/sthjzk/ydyhjgl/': 'report',
            'http://www.mee.gov.cn/zcwj/zyygwj/': 'policy',
            'http://www.mee.gov.cn/zcwj/gwywj/': 'policy',
            'http://www.mee.gov.cn/zcwj/bwj/ling/': 'policy',
            'http://www.mee.gov.cn/zcwj/bwj/gg/': 'policy',
            'http://www.mee.gov.cn/zcwj/bwj/wj/': 'policy',
            'http://www.mee.gov.cn/zcwj/bgtwj/wj/': 'policy',
            'http://www.mee.gov.cn/zcwj/xzspwj/': 'policy',
            'http://www.mee.gov.cn/zcwj/haqjwj/wj/': 'policy',
        }

        for url, category in start_dict.items():
            if category == 'report':
                yield scrapy.Request(url, callback=self.report_parser)
            else:
                yield scrapy.Request(url, callback=self.policy_parser)

    def report_parser(self, response):
        hasbeen_flip = response.meta.get('hasbeen_flip')
        lis = response.xpath("//ul[@id='div']/li")
        for li in lis:
            item = PolicyItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'policy'
            item['target'] = '生态环境部'
            item['domain'] = 'www.mee.gov.cn'
            item['project'] = 1
            item['title'] = li.xpath("./a/text()").extract_first()
            item['init_time'] = datetime.now()
            publish_time = li.xpath(
                "./span[@class='date']/text()").extract_first()
            item['publish_time'] = datetime.strptime(publish_time, '%Y-%m-%d')
            item['uuid'] = str(uuid.uuid1())
            url = li.xpath("./a/@href").extract_first()
            if url.startswith('http'):
                item['url'] = url
            else:
                if response.url.endswith('tml'):
                    item['url'] = re.findall(r".*/", response.url)[0] + url[2:]
                else:
                    item['url'] = response.url + url[2:]
            if item['url'].endswith('pdf'):
                item['file_urls'] = [item['url']]
                yield item
            else:
                yield scrapy.Request(item['url'], callback=self.report_detail_parser, meta={'item': item.copy()})
        if not hasbeen_flip:
            count_pages = re.findall(
                r'createPageHTML\((\d+), 0, "index", "shtml"\);', response.text)
            count_pages = int(count_pages[0])
            if count_pages > 1:
                for i in range(1, count_pages):
                    next_url = response.url + 'index_{}.shtml'.format(i)
                    yield scrapy.Request(next_url, callback=self.report_parser, meta={'hasbeen_flip': True})

    def report_detail_parser(self, response):
        item = response.meta.get('item')
        item['content'] = response.xpath(
            "string(//div[@class='TRS_Editor'])").extract_first()
        item['content_html'] = response.xpath(
            "//div[@class='TRS_Editor']").extract_first()
        keyword = extract_tags(
            item['content'], topK=2, allowPOS=('n', 'v', 'vn'))
        item['keyword'] = ";".join(keyword)
        yield item

    def policy_parser(self, response):
        hasbeen_flip = response.meta.get('hasbeen_flip')
        lis = response.xpath("//ul[@id='div']/li")
        for li in lis:
            item = PolicyItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'policy'
            item['target'] = '生态环境部'
            item['domain'] = 'www.mee.gov.cn'
            item['project'] = 1
            item['title'] = li.xpath("./a/text()").extract_first()
            item['init_time'] = datetime.now()
            publish_time = li.xpath(
                "./span[@class='date']/text()").extract_first()
            item['publish_time'] = datetime.strptime(publish_time, '%Y-%m-%d')
            item['uuid'] = str(uuid.uuid1())
            url = li.xpath("./a/@href").extract_first()
            if url.startswith('./'):
                if response.url.endswith('tml'):
                    item['url'] = re.findall(r".*/", response.url)[0] + url[2:]
                else:
                    item['url'] = response.url + url[2:]
            elif url.startswith('../../..'):
                item['url'] = 'http://www.mee.gov.cn/' + url[9:]
            yield scrapy.Request(item['url'], callback=self.policy_detail_parser, meta={'item': item.copy()})
        if not hasbeen_flip:
            count_pages = re.findall(
                r'createPageHTML\((\d+), 0, "index", "shtml"\);', response.text)
            count_pages = int(count_pages[0])
            if count_pages > 1:
                for i in range(1, count_pages):
                    next_url = response.url + 'index_{}.shtml'.format(i)
                    yield scrapy.Request(next_url, callback=self.policy_parser, meta={'hasbeen_flip': True})

    def policy_detail_parser(self, response):
        item = response.meta.get('item')
        top_box = response.xpath(
            "//div[@class='content_top_box']").extract_first()
        if top_box:
            item['issued_number'] = response.xpath(
                "//div[@class='content_top_box']//ul/li[@class='last']/div[1]/text()").extract_first()
            item['source'] = response.xpath(
                "//div[@class='content_top_box']//ul/li[@class='last']/preceding-sibling::li[1]/div[1]/i/text()").extract_first()
            if not item['source']:
                item['source'] = response.xpath(
                    "//div[@class='content_top_box']//ul/li[@class='last']/preceding-sibling::li[1]/div[1]/text()").extract_first()
            item['area'] = response.xpath(
                "//div[@class='content_top_box']//ul/li[@class='last']/preceding-sibling::li[2]/div[2]/text()").extract_first()
            item['content_html'] = response.xpath(
                "//div[@class='content_body_box']").extract_first()
            try:
                item['content'] = response.xpath(
                    "string(//div[@class='content_body_box'])").extract_first().strip()
            except:
                item['content'] = response.xpath(
                    "string(//div[@class='content_body_box'])").extract_first()
        else:
            source = response.xpath(
                "//div[@class='wjkFontBox']/em[2]/text()").extract_first()
            try:
                item['source'] = re.findall(r"来源：(.*)", source)[0]
            except:
                item['source'] = None
            item['content_html'] = response.xpath(
                "//div[@class='TRS_Editor']").extract_first()
            item['content'] = response.xpath(
                "string(//div[@class='TRS_Editor'])").extract_first()
            issued_number = re.findall(
                r"\S+〔\d+〕\d+号|\S+?公告\d+年第\d+号", item['content'])
            if issued_number:
                item['issued_number'] = issued_number[0]
        yield item


class KapsarcSpider(scrapy.Spider):
    name = 'kapsarc'
    allowed_domains = ['kapsarc.org', 'mdpi.com', 'scopus.com', 'sciencedirect.com',
                       'researchportal.port.ac.uk', 'springer.com', 'scientific-journal.com', 't20japan.org']
    start_urls = [
        'https://www.kapsarc.org/research/publications/?filter-date-from=&filter-date-to=&q=']

    def parse(self, response):
        divs = response.xpath(
            "//div[@class='results-list']/article/div[@class='publication-content']")
        for div in divs:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'en'
            item['organization'] = '阿卜杜拉石油研究中心'
            item['target'] = 'www.kapsarc.org'
            item['project'] = 1
            item['uuid'] = str(uuid.uuid1())
            item['title'] = div.xpath(
                ".//a[@title='Publication title']/text()").extract_first()
            item['url'] = div.xpath(
                ".//a[@title='Publication title']/@href").extract_first()
            item['author'] = div.xpath(
                ".//p[@class='publication-authors-list']/text()").extract_first()
            if item['author']:
                item['author'] = item['author'].replace(",", ";")
            item['init_time'] = datetime.now()
            publish_time = div.xpath(".//small/text()").extract_first()
            if len(publish_time) > 4:
                item['publish_time'] = datetime.strptime(
                    publish_time, "%B %d, %Y")
            else:
                item['publish_time'] = datetime.strptime(publish_time, "%Y")
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
            if item['url'].endswith('pdf'):
                item['file_urls'] = [item['url']]
                yield item
            else:
                yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})
        next_page = response.xpath(
            "//a[@class='pager-right']/@href").extract_first()
        if next_page:
            yield scrapy.Request(next_page, callback=self.parse)

    def detail_parser(self, response):
        item = response.meta.get('item')
        if 'mdpi' in response.url:
            abstract = response.xpath(
                "//h2[text()='Abstract']/../following-sibling::div/div[contains(@class,'art-abstract')]/text()").extract()
            abstract = " ".join(abstract)
            item['abstract'] = abstract.strip()
            item['content'] = item['abstract']
            item['keyword'] = response.xpath(
                "//*[@itemprop='keywords']/text()").extract_first()
            file_link = response.xpath(
                "//a[contains(@class,'PDF')]/@href").extract_first()
            if not file_link.startswith('http'):
                file_link = "https://www.mdpi.com" + file_link
            item['file_urls'] = [file_link]
        elif 'port.ac.uk' in response.url:
            item['abstract'] = response.xpath(
                "string(//div[@class='textblock'])").extract_first()
            item['content'] = item['abstract']
            item['volume'] = response.xpath(
                "//th[text()='Volume']/following-sibling::td/text()").extract_first()
            item['journals_title'] = response.xpath(
                "//th[text()='Journal']/following-sibling::td//text()").extract_first()
            file_link = response.xpath(
                "//a[@class='link title']/@href").extract_first()
            item['file_urls'] = [file_link]
        elif 'springer' in response.url:
            item['abstract'] = response.xpath(
                "string(//div[@id='Abs1-content'])").extract_first()
            item['content'] = item['abstract']
        elif 'scopus' in response.url:
            item['abstract'] = response.xpath(
                "//section[@id='abstractSection']/p[1]/text()").extract_first()
            item['content'] = item['abstract']
            keyword = response.xpath(
                "//section[@id='authorKeywords']/span/text()").extract()
            item['keyword'] = ";".join(keyword)
        elif 'kapsarc' in response.url:
            tags = response.xpath(
                "//span[contains(text(),'Tags')]/following-sibling::ul/li/a/text()").extract()
            item['tags'] = ";".join(tags)
            item['keyword'] = item['tags']
            abstract = response.xpath(
                "//div[contains(@class,'abstract')]/p/text()").extract()
            item['abstract'] = " ".join(abstract)
            item['content'] = item['abstract']
            file_link = response.xpath(
                "//a[@id='share-clipboard']/@href").extract_first()
            if file_link.endswith('='):
                file_link = response.xpath(
                    "//a[contains(@href,'pdf')]/@href").extract_first()
                if file_link:
                    item['file_link'] = [file_link]
            else:
                file_link = "https://www.kapsarc.org" + file_link
                item['file_urls'] = [file_link]
        else:
            return
        yield item


class EuropaSpider(scrapy.Spider):
    name = 'europa'
    allowed_domains = ['europa.eu']
    start_urls = [
        'https://ec.europa.eu/info/publications_en?field_publication_type_tid_i18n=127&field_core_nal_countries_tid_i18n=All&field_core_departments_target_id_entityreference_filter=All',
        'https://ec.europa.eu/info/publications_en?field_publication_type_tid_i18n=129&field_core_nal_countries_tid_i18n=All&field_core_departments_target_id_entityreference_filter=All',
        'https://ec.europa.eu/commission/presscorner/api/search?language=en&ts=1591239894571&policyarea=43&pagesize=100&pagenumber=1',
        'https://ec.europa.eu/commission/presscorner/api/search?language=en&ts=1591239894571&policyarea=43&pagesize=100&pagenumber=2',
        'https://data.europa.eu/euodp/en/data/dataset?sort=views_total+desc'
    ]

    def parse(self, response):
        if 'publications' in response.url:
            divs = response.xpath(
                "//ul[@class='listing listing--teaser']/li//div[@class='listing__column-main ']")
            for div in divs:
                item = ReportItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'report'
                item['language'] = 'en'
                item['organization'] = '欧盟委员会'
                item['target'] = 'europa.eu'
                item['project'] = 1
                item['uuid'] = str(uuid.uuid1())
                publish_time = div.xpath(
                    "./div[@class='meta']/span[2]/text()").extract_first()
                if publish_time[0] in [str(x) for x in range(10)]:
                    item['publish_time'] = datetime.strptime(
                        publish_time, "%d %B %Y")
                else:
                    item['publish_time'] = datetime.strptime(
                        publish_time, "%B %Y")
                item['year'] = datetime.strftime(item['publish_time'], "%Y")
                item['init_time'] = datetime.now()
                item['title'] = div.xpath("./h3/a/@title").extract_first()
                url = div.xpath("./h3/a/@href").extract_first()
                item['url'] = "https://ec.europa.eu" + url
                yield scrapy.Request(item['url'], callback=self.report_detail_parser, meta={'item': item.copy()})
            next_page = response.xpath(
                "//a[@title='Go to next page']/@href").extract_first()
            if next_page:
                next_page = "https://ec.europa.eu" + next_page
                yield scrapy.Request(next_page, callback=self.parse)
        elif 'data.europa.eu' in response.url:
            lis = response.xpath("//ul[@class='datasets unstyled']/li")
            for li in lis:
                item = ReportItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'report'
                item['language'] = 'en'
                item['organization'] = '欧盟委员会'
                item['target'] = 'europa.eu'
                item['project'] = 1
                item['uuid'] = str(uuid.uuid1())
                item['title'] = li.xpath(
                    "./a[1]/strong/text()").extract_first()
                item['url'] = li.xpath("./a[1]/@href").extract_first()
                yield scrapy.Request(item['url'], callback=self.report_detail_parser2, meta={'item': item.copy()})
            next_page = response.xpath("//a[text()='►']/@href").extract_first()
            if next_page:
                yield scrapy.Request(next_page, callback=self.parse)
        else:
            res = json.loads(response.text)
            docs = res['docuLanguageListResources']
            for doc in docs:
                item = NewsItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'news'
                item['language'] = 'en'
                item['target'] = '欧盟委员会'
                item['domain'] = 'europa.eu'
                item['project'] = 1
                item['uuid'] = str(uuid.uuid1())
                item['title'] = doc['title']
                publish_time = doc['eventDate']
                item['publish_time'] = datetime.strptime(
                    publish_time, "%Y-%m-%d")
                item['init_time'] = datetime.now()
                item['abstract'] = doc['leadText']
                tail = doc['refCode']
                item['url'] = "https://ec.europa.eu/commission/presscorner/api/documents?reference={}&language=en&ts={}".format(
                    tail, int(time.time()*1000))
                yield scrapy.Request(item['url'], self.news_detail_parser, meta={'item': item.copy()})

    def report_detail_parser(self, response):
        item = response.meta.get('item')
        item['author'] = response.xpath(
            "//span[contains(text(),'Author')]/following-sibling::div//text()").extract_first()
        abstract = response.xpath(
            "//div[@class='container-fluid']/div[@class='row'][2]//div[@class='section__group ']/div/div/p/text()").extract()
        item['abstract'] = "\n".join(abstract)
        item['content'] = item['abstract']
        keywords = rake.run(item['abstract'])
        keywords = [tuple[0] for tuple in keywords[:2]]
        item['keyword'] = ';'.join(keywords)
        file_link = response.xpath(
            "//a[contains(@class,'piwik_download')]/@href").extract_first()
        if file_link and file_link.endswith(".pdf"):
            item['file_urls'] = [file_link]
        yield item

    def news_detail_parser(self, response):
        item = response.meta.get('item')
        res = json.loads(response.text)
        item['subtitle'] = res['docuLanguageResource']['subtitle']
        item['content_html'] = res['docuLanguageResource']['htmlContent']
        html = etree.HTML(item['content_html'])
        item['content'] = html.xpath('string(.)')
        yield item

    def report_detail_parser2(self, response):
        item = response.meta.get('item')
        abstract = response.xpath(
            "string(//h2[contains(text(),'Description')]/following-sibling::div)").extract_first()
        if abstract:
            item['abstract'] = abstract.strip()
            item['content'] = item['abstract']
            keywords = rake.run(item['abstract'])
            keywords = [tuple[0] for tuple in keywords[:2]]
            item['keyword'] = ';'.join(keywords)
        publish_time = response.xpath(
            "//dt[text()='Release Date']/following-sibling::dd/text()").extract_first()
        try:
            item['publish_time'] = datetime.strptime(publish_time, "%Y-%m-%d")
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
        except:
            item['publish_time'] = None
            item['year'] = None
        item['init_time'] = datetime.now()
        update_time = response.xpath(
            "//dt[text()='Modified Date']/following-sibling::dd/text()").extract_first()
        try:
            item['update_time'] = datetime.strptime(update_time, "%Y-%m-%d")
        except:
            pass
        file_links = response.xpath(
            "//div[@id='dataset-resources']//ul[contains(@class,'resource-list')]//a[@class='button-box']/@href").extract()
        if len(file_links) > 5:
            item['file_urls'] = file_links[:5]
        else:
            item['file_urls'] = file_links
        yield item


class CtiaSpider(scrapy.Spider):
    name = 'ctia'
    allowed_domains = ['ctia.org']
    start_urls = [
        'https://www.ctia.org/api/wordpress/posts/?categories=report&page=1&limit=48']

    def parse(self, response):
        res = json.loads(response.text)
        items = res.get('items')
        for r in items:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'en'
            item['organization'] = '无线产业'
            item['target'] = 'www.ctia.org'
            item['project'] = 1
            item['uuid'] = str(uuid.uuid1())
            item['title'] = r['title']
            item['keyword'] = ";".join([x['name'] for x in r['tags']])
            if r.get('author'):
                item['author'] = r['author']['name']
            item['publish_time'] = datetime.strptime(
                r['date'], "%Y-%m-%dT%H:%M:%S")
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
            item['init_time'] = datetime.now()
            item['url'] = "https://www.ctia.org/news/" + r['slug']
            fields = r.get('fields')
            if fields:
                try:
                    file_link = r['fields'].get('document').get(
                        'link_to').get('file').get('url')
                except:
                    file_link = r['fields'].get(
                        'document').get('link_to').get('url')
                item['file_urls'] = [file_link]
                item['abstract'] = r['fields'].get('meta_description')
                if not item['abstract']:
                    abstracts = fields.get('components')
                    try:
                        abstract = " ".join([d['text'] for d in abstracts])
                        html = etree.HTML(abstract)
                        item['abstract'] = html.xpath("string(//*)").strip()
                    except:
                        item['abstract'] = None
                item['content'] = item['abstract']
            yield item
        total_pages = int(res['totalPages'])
        if total_pages > 1:
            for page in range(2, total_pages + 1):
                url = 'https://www.ctia.org/api/wordpress/posts/?categories=report&page={}&limit=48'.format(
                    page)
                yield scrapy.Request(url, callback=self.parse)


class OrnlSpider(scrapy.Spider):
    name = 'ornl'
    allowed_domains = ['ornl.gov']
    start_urls = ['https://www.ornl.gov/news']

    def parse(self, response):
        divs = response.xpath(
            "//div[@class='news-all_news']//div[@class='pure-u-1']/div[@class='pure-g']")
        for div in divs:
            item = NewsItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'news'
            item['language'] = 'en'
            item['target'] = '橡树岭国家实验室'
            item['domain'] = 'www.ornl.gov'
            item['project'] = 1
            item['uuid'] = str(uuid.uuid1())
            item['title'] = div.xpath(
                ".//div[@class='list-item-title']//a/text()").extract_first()
            url = div.xpath(
                ".//div[@class='list-item-title']//a/@href").extract_first()
            if not url.startswith('http'):
                url = 'https://www.ornl.gov' + url
            item['url'] = url
            publish_time = div.xpath(".//time/text()").extract_first()
            item['publish_time'] = datetime.strptime(publish_time, "%B %d, %Y")
            item['init_time'] = datetime.now()
            item['abstract'] = div.xpath(
                "string(.//div[@class='list-item-desc'])").extract_first()
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})
        flip = response.meta.get('flip')
        if not flip:
            last_page_link = response.xpath(
                "//a[@title='Go to last page']/@href").extract_first()
            total_page = int(re.findall(r"\d+$", last_page_link)[0])
            for page in range(1, total_page + 1):
                page_url = "https://www.ornl.gov/news?search_api_fulltext=&page={}".format(
                    page)
                yield scrapy.Request(page_url, callback=self.parse, meta={'flip': True})

    def detail_parser(self, response):
        item = response.meta.get('item')
        keyword = response.xpath(
            "//div[@class='section-topic']/span[@class='topic']//a/text()").extract()
        item['keyword'] = ";".join(keyword)
        content = response.xpath("string(//article[@class='node node--type-news node--view-mode-full']/div[@class='node__content']/div[@class='pure-g field-wrapper'][2]/div[@class='field-container']/div[@class='pure-u-1 field-elements']/div[@class='pure-u-1-1 field-element']/div[@class='paragraph paragraph--type--text-body paragraph--view-mode--default']/div[@class='pure-g field-wrapper']/div[@class='field-container']/div[@class='pure-u-1-1 field-element'])").extract()
        if content:
            item['content'] = "\n".join(content)
            content_html = response.xpath("//article[@class='node node--type-news node--view-mode-full']/div[@class='node__content']/div[@class='pure-g field-wrapper'][2]/div[@class='field-container']/div[@class='pure-u-1 field-elements']/div[@class='pure-u-1-1 field-element']/div[@class='paragraph paragraph--type--text-body paragraph--view-mode--default']/div[@class='pure-g field-wrapper']/div[@class='field-container']/div[@class='pure-u-1-1 field-element']").extract()
            item['content_html'] = "\n".join(content_html)
        else:
            content = response.xpath(
                "string(//div[@class='image-description'])").extract_first()
            if content:
                item['content'] = content.strip()
                item['content_html'] = response.xpath(
                    "//div[@class='image-description']").extract_first()
            else:
                item['content'] = response.xpath(
                    "string(//article[@class='node node--type-news node--view-mode-full']/div[@class='node__content']/div[@class='pure-g field-wrapper field-body']/div[@class='field-container']/div[@class='pure-u-1-1 field-element'])").extract_first()
                item['content_html'] = response.xpath(
                    "//article[@class='node node--type-news node--view-mode-full']/div[@class='node__content']/div[@class='pure-g field-wrapper field-body']/div[@class='field-container']/div[@class='pure-u-1-1 field-element']").extract_first()
        yield item


class IufroSpider(scrapy.Spider):
    name = 'iufro'
    allowed_domains = ['iufro.org']
    start_urls = [
        'https://www.iufro.org/publications/news/electronic-news/',
        'https://www.iufro.org/publications/annual-report/',
        'https://www.iufro.org/publications/series/world-series/',
        'https://www.iufro.org/publications/series/research-series/'
    ]

    def parse(self, response):
        if 'publications/news' in response.url:
            divs = response.xpath(
                "//div[@class='items-block']//div[@class='box']")
            for div in divs:
                item = NewsItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'news'
                item['language'] = 'en'
                item['target'] = '国际林业研究组织联盟'
                item['domain'] = 'www.iufro.org'
                item['project'] = 1
                item['uuid'] = str(uuid.uuid1())
                item['title'] = div.xpath("./a/@title").extract_first()
                item['url'] = div.xpath("./a/@href").extract_first()
                yield scrapy.Request(item['url'], callback=self.news_detail_parser, meta={'item': item.copy()})
        else:
            divs = response.xpath(
                "//div[@class='items-block']//div[@class='box']")
            for div in divs:
                item = ReportItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'report'
                item['language'] = 'en'
                item['organization'] = '国际林业研究组织联盟'
                item['target'] = 'www.iufro.org'
                item['project'] = 1
                item['uuid'] = str(uuid.uuid1())
                item['title'] = div.xpath("./a/@title").extract_first()
                item['url'] = div.xpath("./a/@href").extract_first()
                yield scrapy.Request(item['url'], callback=self.report_detail_parser, meta={'item': item.copy()})

    def news_detail_parser(self, response):
        item = response.meta.get('item')
        publish_time = re.findall(
            r"<dd>.(\S*?)<br />", response.text, flags=re.I)
        if publish_time:
            item['publish_time'] = datetime.strptime(
                publish_time[0], "%Y-%m-%d")
        item['init_time'] = datetime.now()
        abstracts = response.xpath(
            "//div[@class='content-columns']//div[@class='news-single-img']/following-sibling::*[not(dl or dd or contains(text(),'To read the full text please choose'))]/text()").extract()
        item['abstract'] = "\n".join(abstracts)
        content_html = response.xpath(
            "//div[@class='content-columns']//div[@class='news-single-img']/following-sibling::*[not(dl or dd or contains(text(),'To read the full text please choose'))]").extract()
        item['content_html'] = "\n".join(content_html)
        file_link = response.xpath(
            "//dl[@class='news-single-files']//a/@href").extract_first()
        item['content'] = file_link
        keywords = rake.run(item['abstract'])
        keywords = [tuple[0] for tuple in keywords[:2]]
        item['keyword'] = ';'.join(keywords)
        yield item

    def report_detail_parser(self, response):
        item = response.meta.get('item')
        abstracts = response.xpath(
            "//div[@class='content-columns']//div[@class='news-single-img']/following-sibling::*[not(dl or dd or contains(text(),'Download '))]/text()").extract()
        item['abstract'] = "\n".join(abstracts)
        item['content'] = item['abstract']
        keywords = rake.run(item['abstract'])
        keywords = [tuple[0] for tuple in keywords[:2]]
        item['keyword'] = ';'.join(keywords)
        publish_time = re.findall(
            r"<dd>.(\S*?)<br />", response.text, flags=re.I)
        if publish_time:
            item['publish_time'] = datetime.strptime(
                publish_time[0], "%Y-%m-%d")
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
        item['init_time'] = datetime.now()
        file_link = response.xpath(
            "//dl[@class='news-single-files']//a/@href").extract_first()
        if file_link:
            item['file_urls'] = [file_link]
        yield item


class EnergySpider(scrapy.Spider):
    name = 'energy'
    allowed_domains = ['energy.gov']
    start_urls = ['https://www.energy.gov/listings/energy-news?page=0']

    def parse(self, response):
        divs = response.xpath(
            "//div[@class='node node-article node-teaser clearfix']")
        for div in divs:
            item = NewsItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'news'
            item['language'] = 'en'
            item['target'] = '美国能源部'
            item['domain'] = 'www.energy.gov'
            item['project'] = 1
            item['uuid'] = str(uuid.uuid1())
            item['title'] = div.xpath("./span/@content").extract_first()
            url = div.xpath("./div/a/@href").extract_first()
            item['url'] = "https://www.energy.gov" + url
            publish_time = div.xpath(
                ".//div[@class='date']/text()").extract_first()
            item['publish_time'] = datetime.strptime(publish_time, "%B %d, %Y")
            item['init_time'] = datetime.now()
            item['abstract'] = div.xpath(
                ".//div[contains(@class,'field-name-field-summary')]/div/div/text()").extract_first()
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})
        next_page = response.xpath(
            "//a[@title='Go to next page']/@href").extract_first()
        if next_page:
            next_url = "https://www.energy.gov" + next_page
            yield scrapy.Request(next_url, callback=self.parse)

    def detail_parser(self, response):
        item = response.meta.get('item')
        strings = response.xpath("//div[@id='main_content']//text()").extract()
        if "###" in strings:
            index = strings.index("###")
            strings = strings[:index]
        item['content'] = ("\n".join(strings)).strip()
        item['content_html'] = response.xpath(
            "//div[@id='main_content']").extract_first()
        keywords = rake.run(item['content'])
        keywords = [tuple[0] for tuple in keywords[:2]]
        item['keyword'] = ';'.join(keywords)
        yield item


class AcatechSpider(scrapy.Spider):
    name = 'acatech'
    allowed_domains = ['acatech.de']
    start_urls = [
        'https://www.acatech.de/wp-json/wp/v2/publication?page=1&per_page=100&post_type=publication',
        'https://www.acatech.de/wp-json/wp/v2/publication?page=2&per_page=100&post_type=publication',
        'https://www.acatech.de/wp-json/wp/v2/publication?page=3&per_page=100&post_type=publication',
        'https://www.acatech.de/wp-json/wp/v2/publication?page=4&per_page=100&post_type=publication'
    ]

    def parse(self, response):
        res = json.loads(response.text)
        for article in res:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'de'
            item['organization'] = '德国科学工程院'
            item['target'] = 'www.acatech.de'
            item['project'] = 1
            item['uuid'] = str(uuid.uuid1())
            item['title'] = article['title']['rendered']
            item['url'] = article['link']
            publish_time = article['date']
            item['publish_time'] = datetime.strptime(
                publish_time, '%Y-%m-%dT%H:%M:%S')
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
            item['init_time'] = datetime.now()
            item['author'] = article['_metainfo']['author']
            content_html = article['content']['rendered']
            html = etree.HTML(content_html)
            content = html.xpath('//text()')
            item['abstract'] = "".join(content)
            item['content'] = item['abstract']
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

    def detail_parser(self, response):
        item = response.meta.get('item')
        file_link = response.xpath(
            "//p[@class='publication_downloads']//a/@href").extract_first()
        item['file_urls'] = [file_link]
        keyword = response.xpath(
            "//a[@class='acabluemiddle']/text()").extract()
        item['keyword'] = ";".join(keyword)
        yield item


class WorldBankSpider(scrapy.Spider):
    name = 'worldbank'
    allowed_domains = ['worldbank.org']
    start_urls = [
        'https://openknowledge.worldbank.org/handle/10986/9/recent-submissions',
        'https://search.worldbank.org/api/v2/news?format=json&rows=20&fct=displayconttype_exact,topic_exact,lang_exact,count_exact,countcode_exact,admreg_exact&src=cq55&apilang=en&displayconttype_exact=Press+Release&lang_exact=English&qterm='
    ]

    def parse(self, response):
        if 'recent-submissions' in response.url:
            divs = response.xpath(
                "//ul[@class='ds-artifact-list list-unstyled']/li//div[@class='item-metadata']")
            for div in divs:
                item = ReportItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'report'
                item['language'] = 'en'
                item['organization'] = '世界银行'
                item['target'] = 'worldbank.org'
                item['project'] = 1
                item['uuid'] = str(uuid.uuid1())
                item['title'] = div.xpath(".//h4/a/text()").extract_first()
                url = div.xpath(".//h4/a/@href").extract_first()
                item['url'] = "https://openknowledge.worldbank.org" + url
                item['author'] = div.xpath(
                    ".//div[@class='content author-info']//a/text()").extract_first()
                publish_time = div.xpath(
                    ".//div[@class='content author-info']//a/span/text()").extract_first()
                item['publish_time'] = datetime.strptime(
                    publish_time, " (%Y-%m)")
                item['year'] = datetime.strftime(item['publish_time'], "%Y")
                item['init_time'] = datetime.now()
                yield scrapy.Request(item['url'], callback=self.report_detail_parser, meta={'item': item.copy()})
            next_page = response.xpath(
                "//a[@class='next-page-link']/@href").extract_first()
            if next_page:
                next_page = "https://openknowledge.worldbank.org" + next_page
                yield scrapy.Request(next_page, callback=self.parse)
        else:
            res = json.loads(response.text)
            total = int(res['total'])
            for rows in range(500, total, 500):
                yield scrapy.FormRequest('https://search.worldbank.org/api/v2/news?format=json&rows={}&fct=displayconttype_exact,topic_exact,lang_exact,count_exact,countcode_exact,admreg_exact&src=cq55&apilang=en&displayconttype_exact=Press+Release&lang_exact=English&qterm='.format(rows), callback=self.news_parser)

    def report_detail_parser(self, response):
        item = response.meta.get('item')
        item['abstract'] = response.xpath(
            "string(//div[@class='col-sm-8']//div[contains(@class,'abstract')])").extract_first()
        item['content'] = item['abstract']
        file_link = response.xpath(
            "//h5[text()='Download']/following-sibling::div/a/@href").extract_first()
        if file_link:
            file_link = "https://openknowledge.worldbank.org" + file_link
            item['file_urls'] = [file_link]
        yield item

    def news_parser(self, response):
        res = json.loads(response.text)
        # facets
        for key, document in res['documents'].items():
            if not key == 'facets':
                item = NewsItem()
                item['crawler_ip'] = get_host_ip()
                item['category'] = 'news'
                item['language'] = 'en'
                item['target'] = '世界银行'
                item['domain'] = 'worldbank.org'
                item['project'] = 1
                item['uuid'] = str(uuid.uuid1())
                item['url'] = document['url']
                item['title'] = document['title']['cdata!']
                item['abstract'] = document['descr']['cdata!']
                item['content'] = document['content']['cdata!']
                if 'country' in document:
                    item['location'] = document['country']
                if 'topic' in document:
                    item['area'] = document['topic']
                if 'keywd' in document:
                    keyword = document['keywd']
                    item['keyword'] = keyword.replace(',', ';')
                publish_time = document['lnchdt']
                item['publish_time'] = datetime.strptime(
                    publish_time, "%Y-%m-%dT%H:%M:%SZ")
                item['init_time'] = datetime.now()
                item['author'] = None
                yield item


class LowyinstituteSpider(scrapy.Spider):
    name = 'lowyinstitute'
    allowed_domains = ['lowyinstitute.org']
    start_urls = ['https://www.lowyinstitute.org/publications/reports']

    def parse(self, response):
        divs = response.xpath(
            "//div[@class='container']/div[@class='row']//div[@class='view-content']/div//div[@class='node-content']/div[contains(@class,'right-content')]")
        for div in divs:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['language'] = 'en'
            item['organization'] = '澳大利亚智库罗伊国际政策研究所(LOWYinstitute)'
            item['target'] = 'lowyinstitute.org'
            item['project'] = 1
            item['uuid'] = str(uuid.uuid1())
            item['title'] = div.xpath(
                "./div[@class='padding-purpose']/h2/a/text()").extract_first()
            url = div.xpath(
                "./div[@class='padding-purpose']/h2/a/@href").extract_first()
            author = div.xpath(
                "./div[@class='padding-purpose']/div[@class='content']/div[@class='submitted']/a/text()").extract()
            item['author'] = ';'.join(author)
            publish_time = div.xpath(
                ".//div[@class='date']/text()").extract_first()
            item['publish_time'] = datetime.strptime(publish_time, "%d %b %y")
            item['year'] = datetime.strftime(item['publish_time'], "%Y")
            item['init_time'] = datetime.now()
            if url.startswith('http'):
                item['url'] = url
                item['abstract'] = div.xpath(
                    "./div[@class='padding-purpose']/div[@class='content']/div[contains(@class,'summary')]/text()").extract_first().strip()
                item['content'] = item['abstract']
                yield item
            else:
                item['url'] = "https://www.lowyinstitute.org" + url
                yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

    def detail_parser(self, response):
        item = response.meta.get('item')
        abstract = response.xpath(
            "string(//div[@class='container pub_article_sections'][1]//div[@class='section_content'])").extract_first()
        if abstract:
            abstract = abstract.strip()
            if abstract:
                item['abstract'] = abstract
                item['content'] = abstract
                keywords = rake.run(item['content'])
                keywords = [tuple[0] for tuple in keywords[:2]]
                item['keyword'] = ';'.join(keywords)
            else:
                abstract = response.xpath(
                    "string(//h2[contains(text(),'summary')]/../following-sibling::div)").extract_first()
                if abstract:
                    item['abstract'] = abstract.strip()
                    item['content'] = abstract.strip()
                    keywords = rake.run(item['content'])
                    keywords = [tuple[0] for tuple in keywords[:2]]
                    item['keyword'] = ';'.join(keywords)
        else:
            abstract = response.xpath(
                "string(//h2[contains(text(),'summary') or contains(text(),'Introduction')]/../following-sibling::div)").extract_first()
            if abstract:
                item['abstract'] = abstract.strip()
                item['content'] = abstract.strip()
                keywords = rake.run(item['content'])
                keywords = [tuple[0] for tuple in keywords[:2]]
                item['keyword'] = ';'.join(keywords)

        file_link = response.xpath(
            "//a[contains(@class,'download_link')]/@href").extract_first()
        if file_link:
            item['file_urls'] = [file_link]
        yield item


class EfdinitiativeSpider(scrapy.Spider):
    name = 'efdinitiative'
    allowed_domains = ['efdinitiative.org', 'springer.com', 'doi.org']
    start_urls = [
        'https://www.efdinitiative.org/publications?author=All&page=0']

    def parse(self, response):
        flip = response.meta.get('flip')
        if not flip:
            total_pages = response.xpath(
                "//a[@title='Go to last page']/@href").extract_first()
            total_pages = re.findall(r"\d+$", total_pages)[0]
            for n in range(1, int(total_pages) + 1):
                yield scrapy.Request("https://www.efdinitiative.org/publications?author=All&page={}".format(n), callback=self.parse, meta={'flip': 1})
        divs = response.xpath("//div[@class='views-row']")
        for div in divs:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['organization'] = '环境发展倡议组织'
            item['target'] = 'efdinitiative.org'
            item['project'] = 1
            item['uuid'] = str(uuid.uuid1())
            item['title'] = div.xpath("./div/h2/a/text()").extract_first()
            url = div.xpath("./div/h2/a/@href").extract_first()
            item['url'] = "https://www.efdinitiative.org" + url
            publish_time = div.xpath(
                ".//span[@class='date']/text()[1]").extract_first()
            item['publish_time'] = datetime.strptime(
                publish_time, "%d %B %Y | ")
            item['init_time'] = datetime.now()
            author = div.xpath(
                ".//span[@class='field-content']/p/text()").extract_first()
            author_list = author.split('\n')
            author_str = author_list[1]
            item['author'] = author_str[:-1].replace(', ', ';')
            year_str = author_list[2]
            try:
                item['year'] = re.findall(r"\d+", year_str)[0]
            except:
                item['year'] = datetime.strftime(item['publish_time'], "%Y")
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})

    def detail_parser(self, response):
        item = response.meta.get('item')
        item['abstract'] = response.xpath(
            "string(//div[@class='field field--name-body field--type-text-with-summary field--label-hidden field__item'])").extract_first()
        item['content'] = item['abstract']
        item['language'] = langid.classify(item['abstract'])[0]
        keywords = rake.run(item['content'])
        keywords = [tuple[0] for tuple in keywords[:2]]
        item['keyword'] = ';'.join(keywords)
        tags = response.xpath(
            "//div[@class='buttons field field--name-field-themes field--type-entity-reference field--label-above']//a/text()").extract()
        item['tags'] = ";".join(tags)
        location = response.xpath(
            "//div[text()='Country']/following-sibling::div//a/text()").extract_first()
        if location:
            item['location'] = location
        file_link = response.xpath(
            "//h3[text()='Files and links']/following-sibling::div//a/@href").extract_first()
        if file_link:
            if file_link.endswith(".pdf"):
                item['file_urls'] = [file_link]
        yield item
    #         else:
    #             yield scrapy.Request(file_link,callback=self.file_parser, meta={'item': item.copy()})

    # def file_parser(self,response):
    #     item = response.meta.get('item')
    #     if 'unal.edu' in response.url:
    #         file_view = response.xpath("//a[@class='file' and text()='PDF']/@href").extract_first()
    #         file_link = file_view.replace("view","download")

    #     item['file_urls'] = [file_link]


class EpaSpider(scrapy.Spider):
    name = 'epa'
    allowed_domains = ['epa.gov','psu.edu','ncbi.nlm.nih.gov','cmascenter.org']
    start_urls = ['https://cfpub.epa.gov/si/si_lab_search_results.cfm?\
        subject=Water%20Research&showCriteria=0&searchAll=water%20and%20\
            (resources%20or%20quality%20or%20security%20or%20contaminants\
                %20or%20distribution%20or%20treatment%20or%20hydrologic\
                    %20flow%20or%20estuaries%20or%20watersheds%20or%20wetlands\
                        %20or%20aquatic%20ecosystems%20or%20ground%20water%20or\
                            %20drinking%20water%20or%20green%20infrastructure%20\
                                or%20wastewater%20or%20hydraulic%20fracturing%20or\
                                    %20stormwater%20or%20microbial%20or%20waterborne\
                                        %20virus%20or%20waterborne%20illness%20or%20nutrient\
                                            %20pollution%20or%20mountaintop%20mining)&sortBy=revisionDate&dateBeginPublishedPresented=01%2F01%2F2010']

    def parse(self, response):
        divs = response.xpath("//div[@style='padding-bottom:1em;']")
        for div in divs:
            item = ReportItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'report'
            item['organization'] = '美国环境保护署'
            item['target'] = 'cfpub.epa.gov'
            item['project'] = 1
            item['uuid'] = str(uuid.uuid1())
            item['language'] = 'en'
            item['title'] = div.xpath("./a/text()").extract_first()
            url = div.xpath("./a/@href").extract_first()
            item['url'] = "https://cfpub.epa.gov/si/" + url
            yield scrapy.Request(item['url'], callback=self.detail_parser, meta={'item': item.copy()})
        next_page = response.xpath("//a[contains(text(),'Next >>')]/@href").extract_first()
        if next_page:
            next_page = 'https://cfpub.epa.gov/si/' + next_page
            yield scrapy.Request(next_page, callback=self.parse)

    def detail_parser(self,response):
        item = response.meta.get('item')
        item['author'] = None
        item['abstract'] = response.xpath("string(//h4[text()='Description:']/following-sibling::p)").extract_first()
        item['content'] = item['abstract']
        item['init_time'] = datetime.now()
        publish_time = response.xpath("//b[text()='Product Published Date: ']/../text()[2]").extract_first()
        item['publish_time'] = datetime.strptime(publish_time,"%m/%d/%Y ")
        item['year'] = datetime.strftime(item['publish_time'], "%Y")
        keywords = rake.run(item['content'])
        keywords = [tuple[0] for tuple in keywords[:2]]
        item['keyword'] = ';'.join(keywords)
        links = response.xpath("//h4[text()='URLs/Downloads:']/following-sibling::a/@href").extract()
        for link in links:
            if 'psu.edu' in link or 'ncbi.nlm.nih.gov' in link:
                yield scrapy.Request(link,callback=self.file_parser,meta={'item': item.copy()})
                break
            elif 'si_public_file_download' in link:
                item['file_urls'] = ["https://cfpub.epa.gov/si/" + link]
                break
        yield item

    def file_parser(self, response):
        item = response.meta.get('item')
        if 'psu.edu' in response.url:
            file_link = response.xpath("//span[@class='file']/a/@href").extract_first()
            if file_link:
                item['file_urls'] = [file_link]
        elif 'ncbi.nlm.nih.gov' in response.url:
            file_link = response.xpath("//a[contains(text(),'PDF')]/@href").extract_first()
            if file_link:
                if not file_link.startswith("http"):
                    file_link = 'https://www.ncbi.nlm.nih.gov' + file_link
                item['file_urls'] = [file_link]
        yield item

class MofcomSpider(scrapy.Spider):
    name = 'mofcom'
    allowed_domains = ['mofcom.gov.cn']

    def start_requests(self):
        yield scrapy.FormRequest("http://tradeinservices.mofcom.gov.cn/TradeGuide_CMS/tradeFront/yanjiu/pinglun/list",formdata={"pageNo":"1"})

    def parse(self, response):
        flip = response.meta.get('flip')
        res = json.loads(response.text)
        if not flip:
            pages = int(res['maxPageNum'])
            for page in range(2 ,pages + 1):
                data = {"pageNo":"{}".format(page)}
                yield scrapy.FormRequest("http://tradeinservices.mofcom.gov.cn/TradeGuide_CMS/tradeFront/yanjiu/pinglun/list",formdata=data,meta={"flip":1})
        rows = res['rows']
        for row in rows:
            item = NewsItem()
            item['crawler_ip'] = get_host_ip()
            item['category'] = 'news'
            item['language'] = 'zh'
            item['target'] = '商务部公共商务信息平台'
            item['domain'] = 'tradeinservices.mofcom.gov.cn'
            item['project'] = 1
            item['uuid'] = str(uuid.uuid1())
            item['title'] = row['title']
            try:
                item['keyword'] = row['keyword'].replace(',',';')
            except:
                item['keyword'] = None
            item['abstract'] = row['digest']
            publish_time = row['publishTimeStr']
            item['publish_time'] = datetime.strptime(publish_time,"%Y-%m-%d %H:%M:%S")
            item['init_time'] = datetime.now()
            url1 = row['fullNameEN']
            url2 = row['htmlUrl']
            item['url'] = "http://tradeinservices.mofcom.gov.cn/article" + url1 + url2
            yield scrapy.Request(item['url'],callback=self.detail_parser, meta={'item': item.copy()})
    
    def detail_parser(self,response):
        item = response.meta.get('item')
        item['source'] = re.findall(r"var source = '(.*)'",response.text)[0]
        item['content_html'] = response.xpath("//div[@class='article-con-01']").extract_first()
        content = response.xpath("//div[@class='article-con-01']//p//text()").extract()
        item['content'] = "\n".join(content)
        yield item

