#!/usr/bin/python
# -*- coding: UTF-8 -*-


class WikiModel(object):
    wiki_id = ''
    wiki_url = ''
    year = 0
    school = ''
    grade = 0
    _class = ''
    group_no = ''
    class_name = ''
    admin_key = ''

    def __init__(self, wiki_id, wiki_url, year, school, grade, _class, group_no, class_name, admin_key):
        self.wiki_id = wiki_id
        self.wiki_url = wiki_url
        self.year = year
        self.school = school
        self.grade = grade
        self._class = _class
        self.group_no = group_no
        self.class_name = class_name
        self.admin_key = admin_key
