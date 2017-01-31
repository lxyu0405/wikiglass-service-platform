#!/usr/bin/python
# -*- coding: utf-8 -*-
import re


class CharTools(object):
    @staticmethod
    def clean_html_tags(raw_html_content):
        cleaner = re.compile('<.*?>')
        return re.sub(cleaner, '', raw_html_content)

    @staticmethod
    def clean_nbsp(raw_content):
        cleaner = re.compile('&nbsp;')
        return re.sub(cleaner, '', raw_content)

    @staticmethod
    def clean_spaces(raw_content):
        # clear meaningless space
        cleaner = re.compile('\s+')
        return re.sub(cleaner, '', raw_content)

    @staticmethod
    def clean_alph(content):
        cleaner = re.compile('[a-zA-Z]')
        return re.sub(cleaner, '', content)

    @staticmethod
    def clean_meaningless_symbol(raw_content):
        # clear meaningless dot in Content
        cleaner_dot = re.compile('\.{7,}')
        cleaner_dot_text = re.sub(cleaner_dot, ' ', raw_content)
        # clear meaningless - in Content
        content = cleaner_dot_text.replace('-', '').replace('_', '').replace(u'●', u'。')
        return content

    @staticmethod
    def check_contain_chinese(check_str):
        for ch in check_str.decode('utf-8'):
            if u'\u4e00' <= ch <= u'\u9fff':
                return True
                break
        return False

    @staticmethod
    def average_sentences_length(content):
        period_cnt = content.count(u'。')
        comma_cnt = content.count(',') + content.count(u'，')
        question_mark_cnt = content.count('?') + content.count(u'？')
        return len(content) / (period_cnt + comma_cnt + question_mark_cnt + 1)
