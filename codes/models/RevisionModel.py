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

    def __init__(self, revision_id, page_id, timestamp, version, oid, user_id, content, no_of_words, ts):
        self.revision_id = revision_id
        self.page_id = page_id
        self.timestamp = timestamp
        self.version = version
        self.oid = oid
        self.user_id = user_id
        self.content = content
        self.no_of_words = no_of_words
        self.ts = ts