#!/usr/bin/python
# -*- coding: UTF-8 -*-


class PageModel(object):
    page_id = ''
    wiki_id = ''
    page_oid = ''
    page_name = ''
    page_url = ''
    ts = ''

    def __init__(self, page_id, wiki_id, page_oid, page_name, page_url, ts):
        self.page_id = page_id
        self.wiki_id = wiki_id
        self.page_oid = page_oid
        self.page_name = page_name
        self.page_url = page_url
        self.ts = ts
