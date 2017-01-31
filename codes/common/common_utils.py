#!/usr/bin/python
# -*- coding: utf-8 -*-
import urllib
import urlparse
import json
from urllib import urlopen

# diy library
from codes.models.PageModel import PageModel
from codes.models.UserModel import UserModel
from codes.models.RevisionModel import RevisionModel


def fix_url(url):
    # turn string into unicode
    if not isinstance(url, unicode):
        url = url.decode('utf8')

    # parse it
    parsed = urlparse.urlsplit(url)

    # divide the netloc further
    userpass, at, hostport = parsed.netloc.rpartition('@')
    user, colon1, pass_ = userpass.partition(':')
    host, colon2, port = hostport.partition(':')

    # encode each component
    scheme = parsed.scheme.encode('utf8')
    user = urllib.quote(user.encode('utf8'))
    colon1 = colon1.encode('utf8')
    pass_ = urllib.quote(pass_.encode('utf8'))
    at = at.encode('utf8')
    host = host.encode('idna')
    colon2 = colon2.encode('utf8')
    port = port.encode('utf8')
    path = '/'.join(  # could be encoded slashes!
        urllib.quote(urllib.unquote(pce).encode('utf8'), '')
        for pce in parsed.path.split('/')
    )
    query = urllib.quote(urllib.unquote(parsed.query).encode('utf8'), '=&?/')
    fragment = urllib.quote(urllib.unquote(parsed.fragment).encode('utf8'))

    # put it back together
    netloc = ''.join((user, colon1, pass_, at, host, colon2, port))
    return urlparse.urlunsplit((scheme, netloc, path, query, fragment))


class ToolBox(object):
    # prepare data for page list
    @staticmethod
    def get_page_list(wiki_id, wiki_url, wiki_admin_key, page_list):
        if page_list is None:
            page_list = []

        page_url = urlopen(
            wiki_url + "/api_v2/op/GetObjectsNOM/admin_key/" + wiki_admin_key + "/object_types/page").read()
        page_text = page_url.strip('/*-secure- \n')
        page_json_data = json.loads(page_text)

        # Prepare data for Page table
        page_number = page_json_data["_total_page"]
        for i in range(0, page_number):
            page_model = PageModel()
            page_model.page_name = page_json_data["objects"][i]["name"]
            page_model.page_oid = page_json_data["objects"][i]["oid"]
            page_model.page_url = wiki_url + "/w/page/" + str(page_model.page_oid) + "/" + page_model.page_name
            page_model.page_id = wiki_id + "_" + str(i + 1)
            page_model.wiki_id = wiki_id
            # Add the model to the list
            page_list.append(page_model)

    # prepare data for user list
    @staticmethod
    def get_user_list(wiki_url, wiki_admin_key, user_list):
        if user_list is None:
            user_list = []

        user_url = urlopen(wiki_url + "/api_v2/op/GetUsersInfos/admin_key/" + wiki_admin_key + "/verbose/true").read()
        user_text = user_url.strip('/*-secure- \n')
        user_json_data = json.loads(user_text)
        # Prepare data for User table
        users = user_json_data["uids"]
        for usr in users:
            user_model = UserModel()
            user_model.user_id = usr["uid"]
            user_model.perm = usr["perm"]
            if "name" in usr:
                user_model.full_name = usr["name"]
            else:
                user_model.full_name = ''
            if "username" in usr:
                user_model.username = usr["username"]
            else:
                user_model.username = ''
            user_list.append(user_model)

    # prepare data for revision list
    @staticmethod
    def get_revision_list(wiki_url, wiki_admin_key, page_id, page_name, revision_list):
        if revision_list is None:
            revision_list = []

        page_url = urlopen(fix_url(wiki_url + "/api_v2/op/GetPageRevisions/admin_key/" + wiki_admin_key + "/page/" + page_name)).read()
        page_text = page_url.strip('/*-secure- \n')
        page_revision_json_data = json.loads(page_text)

        version = 1
        for tstamp in page_revision_json_data["revisions"]:
            revision_model = RevisionModel()
            revision_model.timestamp = tstamp
            revision_model.page_id = page_id
            revision_model.version = version

            revision_url = urlopen(fix_url(wiki_url + "/api_v2/op/GetPage/admin_key/" + wiki_admin_key + "/page/" + page_name + "/revision/" + str(revision_model.timestamp))).read()
            revision_text = revision_url.strip('/*-secure- \n')
            revision_json_data = json.loads(revision_text)

            if revision_json_data.get('error_string'):
                revision_model.user_id = 'Error'
                revision_model.content = ''
                revision_model.oid = ''
            else:
                revision_model.user_id = revision_json_data["author"]["uid"]
                revision_model.content = revision_json_data["html"].encode('unicode_escape')
                revision_model.oid = revision_json_data["oid"]

            revision_list.append(revision_model)
            version += 1
