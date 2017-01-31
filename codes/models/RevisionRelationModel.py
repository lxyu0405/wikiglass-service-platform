#!/usr/bin/python
# -*- coding: UTF-8 -*-


class RevisionRelationModel(object):
    relation_id = 0
    revision_id = 0
    user_from_id = ''
    user_from_name = ''
    user_to_id = ''
    user_to_name = ''
    page_id = ''
    group_id = ''
    time_stamp = ''

    def __init__(self):
        self.relation_id = 0
        self.revision_id = 0
        self.user_from_id = ''
        self.user_from_name = ''
        self.user_to_id = ''
        self.user_to_name = ''
        self.page_id = ''
        self.group_id = ''
        self.time_stamp = ''