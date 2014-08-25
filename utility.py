#!/usr/bin/env python
# -*- coding:GBK
# time: 2014/03/01 16:25
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:

import re
import math
import pdb, signal

import MySQLdb
import requests

from DataFrame.novel.auth.url_validity_check import *

dir_rid_info_table_num = 10

dir_agg_chapter_info_table_num = 256
dir_ori_stuff_info_table_num = 256

chapter_agg_info_table_num = 256
chapter_agg_link_table_num = 256

chapter_integrate_info_table_num = 256

url_validity_check = UrlValidityCheck()

site_auth_dict = {}


def get_dir_rid_info_table_id(dir_id):
    """
    """

    return dir_id % dir_rid_info_table_num


def get_dir_ori_stuff_info_table_id(rid_id):
    """
    """

    return rid_id % dir_ori_stuff_info_table_num


def get_dir_agg_chapter_info_table_id(rid):
    """
    """

    return rid % dir_agg_chapter_info_table_num


def get_chapter_agg_info_table_id(rid):
    """
    """

    return rid % chapter_agg_info_table_num


def get_chapter_agg_link_table_id(rid):
    """
    """

    return rid % chapter_agg_link_table_num


def get_chapter_integrate_info_table_id(rid):
    """
    """

    return rid % chapter_integrate_info_table_num


def get_fields_from_table(conn, table_name):
    """
    """

    fields_list = []

    if not conn:
        return fields_list

    query_sql = 'SHOW FIELDS FROM {0}'.format(table_name)

    cursor = conn.cursor(MySQLdb.cursors.DictCursor)

    cursor.execute(query_sql)
    rows = cursor.fetchall()
    cursor.close()

    for row in rows:
        fields_list.append(row['Field'])

    return fields_list


def filter_object_attr_by_schema(obj, field_list):
    """
    """

    d = {}
    for field in field_list:

        if field == 'raw_update_time' or field == 'create_time' or field == 'update_time' or field == 'check_time':
            continue

        value = getattr(obj, field, None)
        if value is None:
            continue

        d[field] = value

    return d


def trans_dict_to_update_sql(d):
    """
    """

    update_sql = ''
    value_list = []

    for key, value in d.items():
        update_sql += '{0} = %s,'.format(key)
        value_list.append(value)

    update_sql = update_sql[: -1]

    return update_sql, value_list


def lcp(s1, s2):
    """
    """

    count = 0
    for c1, c2 in zip(s1, s2):
        if c1 != c2:
            break
        count += 1

    return s1[:count]


def calc_url_longest_common_prefix(url1, url2):
    """
    """

    lcp_url = lcp(url1, url2)
    m = re.match('.*[^0-9a-zA-Z]', lcp_url)

    return m.group() if m else ''


def compress_url(url):

    return re.sub('\d+', '0', url)


def debug(signal, frame):
    """
    """

    pdb.set_trace()


def listen():
    """
    """

    signal.signal(signal.SIGUSR1, debug)


def send_get_requests(url, timeout=3):

    try:
        result = requests.get(url, timeout=timeout).json()
    except Exception, e:
        return False

    return result

if __name__ == '__main__':

    url1 = 'http://read.qidian.com/BookReader/3070595,51653269.aspx'
    url2 = 'http://read.qidian.com/BookReader/3070595,51805739.aspx'

    url1 = 'http://vip.book.sina.com.cn/book/chapter_151015_101531.html'

    print calc_url_longest_common_prefix(url1, url2)
    print compress_url(url1)

