#!/usr/bin/env python
# -*- coding:GBK
# time: 2014/03/06 13:01
# mail: lvleibing01@baidu.com
# author: lvleibing01
# desc:


class NovelBase(object):
    """Base class
    """

    def __init__(self, **kwargs):

        self.id = 0

        for key, value in kwargs.items():
            setattr(self, key, value)

    def __del__(self):

        pass


class NovelAggInfo(NovelBase):
    """
    """

    def __init__(self, **kwargs):

        NovelBase.__init__(self, **kwargs)

    def init(self, novel_info):
        """
        """

        self.__init__(id=0, book_name=novel_info.book_name, pen_name=novel_info.pen_name, category=novel_info.category,
                      book_status=novel_info.category, description=novel_info.description, logo=novel_info.logo)


class NovelBasicInfo(NovelBase):
    """
    """

    def __init__(self, **kwargs):

        NovelBase.__init__(self, **kwargs)


if __name__ == '__main__':

    book_ori = BookOri(**{'author': 'lvleibing01', 'email': 'lvleibing01@baidu.com'})

    print book_ori.author
    print book_ori.email
