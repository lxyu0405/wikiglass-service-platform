#!/usr/bin/python
# -*- coding: UTF-8 -*-


class RevisionModel(object):
    revision_id = 0
    page_id = ''
    timestamp = 0
    version = 0
    oid = 0
    user_id = ''
    content = ''
    no_of_words = 0
    ts = ''

    def __init__(self):
        self.revision_id = 0
        self.page_id = ''
        self.timestamp = 0
        self.version = 0
        self.oid = 0
        self.user_id = ''
        self.content = ''
        self.no_of_words = 0
        self.ts = ''