#!/usr/bin/python
# -*- coding: UTF-8 -*-


class UserWikiModel(object):
    user_id = ''
    uid = ''
    wiki_id = ''

    def __init__(self, user_id, uid, wiki_id):
        self.user_id = user_id
        self.uid = uid
        self.wiki_id = wiki_id