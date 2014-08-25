#!/usr/bin/env python
# -*- coding:GBK -*-
# time: 2014/08/25 16:04
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import sys
import MySQLdb

from data_struct import *


class DataLoader(object):

    def __init__(self):

        pass

    def init(self):

        return True

    def init_db(self):

        try:
            self.conn_agg = MySQLdb.connect(host='', port=, user='', passwd='', db='')
        except Exception as e:
            self.conn_agg = None

        try:
            self.conn_auth = MySQLdb.connect(host='', port=, user='', passwd='', db='')
        except Exception as e:
            self.conn_auth = None

        return True

    def __del__(self):

        pass

    def exit(self):

        self.release_db()

        return True

    def release_db(self):

        try:
            self.conn_agg.close()
            self.conn_auth.close()
        except Exception as e:
            pass

        return True

    def load_data(self, gid=0):
        """
        """

        novel_agg_info = self.fetch_novel_agg_info(gid)
        if not novel_agg_info:
            return False

        novel_basic_info = self.construct_novel_basic_info(novel_agg_info)
        if not novel_basic_info:
            return False

        self.dump_novel_basic_info(novel_basic_info)

        return True

    def construct_novel_basic_info(self, novel_agg_info):
        """
        """

        novel_basic_info = None
        if novel_agg_info:
            return novel_basic_info

        novel_basic_info = NovelBase()

        novel_basic_info.book_id = 0
        novel_basic_info.raw_book_id = novel_agg_info.id
        novel_basic_info.cp_id = novel_agg_info.site_id
        novel_basic_info.cp_name = novel_agg_info.site
        novel_basic_info.dir_url = novel_agg_info.dir_url
        novel_basic_info.gid = novel_agg_info.gid
        novel_basic_info.book_name = novel_agg_info.book_name
        novel_basic_info.author_id = 0
        novel_basic_info.author = novel_agg_info.pen_name
        novel_basic_info.category_id = 0
        novel_basic_info.channel = '频道'
        novel_basic_info.category = '-'.join([novel_basic_info.channel, novel_agg_info.category, novel_agg_info.category])
        novel_basic_info.tag = ','.join(['-'.join([novel_basic_info.channel, '分组', tag])
                                         for tag in novel_agg_info.tag.split()])
        novel_basic_info.book_status = novel_agg_info.book_status
        novel_basic_info.description = novel_agg_info.description
        novel_basic_info.logo = novel_agg_info.logo
        novel_basic_info.language = '中文'
        novel_basic_info.format = 'txt'
        novel_basic_info.price_status = 1
        novel_basic_info.price_pattern = 0
        novel_basic_info.price = 10000
        novel_basic_info.chapter_price = 3
        novel_basic_info.public_status = 1
        novel_basic_info.public_time = int(time.time())
        novel_basic_info.ios_public_status = 1
        novel_basic_info.ios_public_time = novel_basic_info.public_time
        novel_basic_info.roll_number = 14
        novel_basic_info.chapter_num = novel_agg_info.chapter_count
        novel_basic_info.public_chapter_num = novel_basic_info.chapter_num
        novel_basic_info.free_chapter_num = int(novel_basic_info.chapter_num * 0.3)
        novel_basic_info.word_sum = 1000 * novel_basic_info.chapter_num
        novel_basic_info.price_word_sum = 1000 * novel_basic_info.public_chapter_num
        novel_basic_info.public_price_word_sum = 1000 * (novel_basic_info.public_chapter_num - novel_basic_info.free_chapter_num)
        novel_basic_info.check_status = novel_basic_info.check_status

        novel_basic_info.last_chapter_index = novel_agg_info.last_chapter_index
        novel_basic_info.last_chapter_id = novel_agg_info.last_chapter_id
        novel_basic_info.last_chapter_url = novel_agg_info.last_chapter_url
        novel_basic_info.last_chapter_title = novel_agg_info.last_chapter_title
        novel_basic_info.last_chapter_update_time = novel_agg_info.last_chapter_update_time
        novel_basic_info.free_last_chapter_index = novel_agg_info.last_chapter_index
        novel_basic_info.free_last_chapter_id = novel_agg_info.last_chapter_id
        novel_basic_info.free_last_chapter_url = novel_agg_info.last_chapter_url
        novel_basic_info.free_last_chapter_title = novel_agg_info.last_chapter_title
        novel_basic_info.free_last_chapter_update_time = novel_agg_info.last_chapter_update_time

        return novel_basic_info

    def fetch_novel_agg_info(self, rid=0):
        """
        """

        novel_agg_info = None

        conn = self.conn_agg
        if not conn:
            return novel_agg_info

        query_sql = 'SELECT * FROM novel_agg_info WHERE rid = %s'

        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query_sql, (rid,))
        row = cursor.fetchone()
        cursor.close()

        if row:
            novel_agg_info = NovelAggInfo(**row)

        return novel_agg_info

    def dump_novel_basic_info(self, novel_basic_info):
        """
        """

        if not novel_basic_info:
            return False

        conn = self.conn_auth
        if not conn:
            return False

        fields_list = get_fields_from_table(conn, 'novel_basic_info_offline'.format())
        if not fields_list:
            return False

        object_attr_dict = filter_object_attr_by_schema(novel_basic_info, fields_list)
        insert_sql = 'INSERT IGNORE INTO novel_basic_info ({0}, create_time, update_time) ' \
                     'VALUES({1}, unix_timestamp(), unix_timestamp())' \
                     ''.format(', '.join(object_attr_dict.keys()), ', '.join(['%s'] * (len(object_attr_dict))))

        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(insert_sql, object_attr_dict.values())
        cursor.close()

        return True

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print 'Usage: {0} gid_file gid_limit'.format(__file__)
        sys.exit(1)

    gid_file = sys.argv[1]
    gid_limit = int(sys.argv[2])

    data_loader = DataLoader()
    data_loader.init()

    count = 0
    for line in open(gid_file):

        if count >= gid_limit:
            break

        line = line.strip()
        if not line:
            continue

        count += 1

        gid = int(line)
        data_loader.load_data(gid)
