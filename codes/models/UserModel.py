#!/usr/bin/python
# -*- coding: UTF-8 -*-


class UserModel(object):
    user_id = ''
    full_name = ''
    username = ''
    perm = ''
    ts = ''

    def __init__(self, user_id, full_name, username, perm, ts):
        self.user_id = user_id
        self.full_name = full_name
        self.username = username
        self.perm = perm
        self.ts = ts