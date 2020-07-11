# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime
from ZHIKU.items import ReportItem,NewsItem,PolicyItem
from scrapy import Item
import re
import pymysql
from scrapy.pipelines.files import FilesPipeline
from scrapy import Request
import os, os.path
import zipfile
import shutil
from ZHIKU.settings import FILES_STORE
import logging
logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)



class ZhikuPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        cls.HOST = crawler.settings.get('MYSQL_HOST', '127.0.0.1')
        cls.PORT = crawler.settings.get('MYSQL_PORT', 3306)
        cls.USER_NAME = crawler.settings.get('MYSQL_USER_NAME', 'root')
        cls.PASSWORD = crawler.settings.get('MYSQL_PASSWORD', '123456')
        cls.DB = crawler.settings.get('MYSQL_DB', 'crawl_p')
        cls.BASE_PATH = os.path.dirname(crawler.settings.get('BASE'))
        return cls()

    def open_spider(self, spider):

        self.conn = pymysql.Connect(host=self.HOST, port=self.PORT, user=self.USER_NAME, password=self.PASSWORD,
                                    db=self.DB)
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()

    def process_item(self, item, spider):
        post = dict(item) if isinstance(item, Item) else item
        if isinstance(item,ReportItem):
            self.insert_sql(post, self.cursor, self.conn, 'journal_report',self.BASE_PATH)
        elif isinstance(item,NewsItem):
            self.insert_sql(post, self.cursor, self.conn, 'news_policy',self.BASE_PATH)
        elif isinstance(item,PolicyItem):
            try:
                self.insert_sql(post, self.cursor, self.conn, 'policy',self.BASE_PATH)
            except:
                self.conn = pymysql.Connect(host=self.HOST, port=self.PORT, user=self.USER_NAME, password=self.PASSWORD,
                                    db=self.DB)
                self.cursor = self.conn.cursor()
                self.insert_sql(post, self.cursor, self.conn, 'policy',self.BASE_PATH)
        return item

    @staticmethod
    def insert_sql(doc, cursor, conn, table,base_path):
        try:
            doc['file_path'] = doc['files'][0]['path']
            doc['file_url'] = doc['files'][0]['url']
            doc['file_name'] = doc['file_path'].split('/')[1]
        except:
            pass
        try:
            del doc['files']
            del doc['file_urls']
        except:
            pass
        if 'useit.com.cn' in doc['url'] or 'cbdio.com' in doc['url'] or 'commerce.gov' in doc['url']:
            doc['file_url'] = doc['url']
            doc['file_path'] = doc['file_path'].split('/')[0] + ".zip"
            doc['file_name'] = doc['file_path']
        if not 'author' in doc.keys() and table!='policy':
            doc['author'] = None
        if not 'abstract' in doc.keys() and table!='policy':
            doc['abstract'] = None
        keys = ','.join(doc.keys())
        values = ','.join(['%s'] * len(doc))
        sql = 'INSERT INTO {table}({keys}) values ({values})'.format(table=table, keys=keys, values=values)
        print(sql)
        try:
            if cursor.execute(sql, tuple(doc.values())):
                conn.commit()
                # 详细日志写入
                if not os.path.exists("/data/logs/crawl"):
                    os.makedirs("/data/logs/crawl")
                logger.handlers = []
                handler = logging.FileHandler("/data/logs/crawl/crawl-{}.log".format(
                    datetime.strftime(datetime.now(), "%Y-%m-%dT%H")))
                handler.setLevel(logging.INFO)
                formatter = logging.Formatter('%(asctime)s\t%(message)s',datefmt="%Y-%m-%d %H:%M:%S")
                handler.setFormatter(formatter)
                logger.addHandler(handler)
                # 通过uuid 获取sql id
                cursor.execute("select id from {table} where uuid='{uuid}'".format(table=table,uuid=doc['uuid']))
                sql_id = cursor.fetchall()
                sql_id = sql_id[0][0] if sql_id[0] else None
                #sql_id uuid 来源 来源地址 类型  采集时间 标题 作者 发布时间 摘要 原文地址
                if table == 'news_policy':
                    log_text = '{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}'.format(
                        sql_id,doc['uuid'],doc['target'],doc['domain'],doc['category'],datetime.strftime(doc['init_time'],"%Y-%m-%d %H:%M:%S"),doc['title'],doc['author'],
                        doc['publish_time'],doc['abstract'],doc['url'])
                elif table == 'journal_report':
                    log_text = '{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}'.format(
                        sql_id,doc['uuid'],doc['organization'], doc['target'], doc['category'], datetime.strftime(doc['init_time'],"%Y-%m-%d %H:%M:%S"), doc['title'], doc['author'],
                        doc['publish_time'], doc['abstract'], doc['url'])
                elif table == 'policy':
                    log_text = '{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}'.format(
                        sql_id,doc['uuid'], doc['target'], doc['category'], datetime.strftime(doc['init_time'],"%Y-%m-%d %H:%M:%S"), doc['title'],
                        doc['publish_time'], doc['url'])
                log_text = re.sub(r"\n","<br>",log_text)
                log_text = re.sub(r"\r","",log_text)
                logger.info(log_text)
        except Exception as e:
            print('##################Failed#################', e)
            conn.rollback()


class ZhikuFilesPipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        return [Request(url, meta={'folder_name': item.get('title'),'file_name':item.get(self.files_urls_field, []).index(url) + 1}) for url in item.get(self.files_urls_field, [])]

    def file_path(self, request, response=None, info=None):
        file_name = request.url.split('/')[-1]
        f = re.findall(r"get_pdf.cfm\?pub_id=|download.cgi\?record_id=|file-download.php\?i=|si_public_file_download.cfm\?p_download_id=", file_name)
        useit = re.findall(r"useit.com.cn|cbdio.com|commerce.gov", request.url)
        if useit:
            type = file_name.split(".")[-1]
            folder_name = request.meta.get('folder_name')
            file_name = request.meta.get('file_name')
            filename =  u'{}/{}'.format(folder_name, file_name) + "." + type
            return filename
        if f:
            folder_strip = re.sub(r'get_pdf.cfm\?pub_id=|download.cgi\?record_id=|file-download.php\?i=|si_public_file_download.cfm\?p_download_id=', '', file_name)
            folder_strip = folder_strip + '-{}.pdf'.format(datetime.strftime(datetime.now(),"%Y%m%d%H%M%S"))
        else:
            folder_strip = file_name
        filename = u'{}'.format(folder_strip)
        if file_name == 'pdf':
            filename = "{}.pdf".format(datetime.strftime(datetime.now(),"%y%m%d%H%M%S%f"))
        if not '.' in file_name:
            filename = "{}.pdf".format(datetime.strftime(datetime.now(),"%y%m%d%H%M%S%f"))
        return filename

    def item_completed(self, results, item, info):
        if isinstance(item, dict) or self.files_result_field in item.fields:
            item[self.files_result_field] = [x for ok, x in results if ok]
        if item['target'] == 'www.useit.com.cn' or item['target'] == 'www.cbdio.com' or item['target'] == 'www.commerce.gov':
            if item['files']:
                folder_name = item['files'][0]['path'].split("/")[0]
                folder = FILES_STORE + "/" + folder_name
                zip = FILES_STORE + "/" + folder_name + ".zip"
                zipDir(folder,zip)
        return item



def zipDir(dirpath,outFullName):
    """
    压缩指定文件夹
    :param dirpath: 目标文件夹路径
    :param outFullName: 压缩文件保存路径+xxxx.zip
    :return: 无
    """
    zip = zipfile.ZipFile(outFullName,"w",zipfile.ZIP_DEFLATED)
    for path,dirnames,filenames in os.walk(dirpath):
        # 去掉目标跟路径，只对目标文件夹下边的文件及文件夹进行压缩
        fpath = path.replace(dirpath,'')

        for filename in filenames:
            zip.write(os.path.join(path,filename),os.path.join(fpath,filename))
    zip.close()

    # 压缩完成后删除文件夹 视情况使用
    shutil.rmtree(dirpath)
