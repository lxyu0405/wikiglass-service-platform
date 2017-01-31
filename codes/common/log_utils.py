#!/usr/bin/python
# -*- coding: utf-8 -*-
import urllib
import urlparse
import json
from urllib import urlopen


class LogTools(object):
    @staticmethod
    def wikiInfo(wiki_id, page_list, user_list, revision_list):
        wiki_log = 'wiki_id :' + wiki_id + '\n'

        page_log = 'page number: ' + str(len(page_list)) + '\n'
        page_log += 'page names: '
        for page in page_list:
            page_log += (page.page_name + ', ')

        user_log = '\nuser number: ' + str(len(user_list)) + '\n'
        user_log += 'user names(write): '
        for user in user_list:
            if user.perm == 'write':
                user_log += (user.full_name + ', ')

        rev_log = '\nrevisions number: ' + str(len(revision_list))

        return wiki_log + page_log + user_log + rev_log