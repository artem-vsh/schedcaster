# -*- coding: utf-8 -*-
"""
Created on Tue Aug  7 18:47:08 2012

@author: avsh
"""

import vkontakte as vk


class ConsumerVK(object):
    def __init__(self, apiId=None, apiSecret=None, token=None, owner=None):
        self.owner = owner

        # try to default to the given token
        self.token = token
        if self.token == None:
            self.api = vk.API(apiId, apiSecret)
            self.token = self.api.token
        else:
            self.api = vk.API(token=token)

    def consume(self, post, attachments=None):
        args = {'message': post}
        if self.owner != None:
            args['owner_id'] = self.owner
            # if id is negative, it's a group, post on its behalf
            if self.owner[0] == '-':
                args['from_group'] = 1
        reply = self.api.wall.post(**args)
        if type(reply) == dict and 'post_id' in reply and\
           reply['post_id'] != None:
            return reply['post_id']
        else:
            raise RuntimeError('wrong reply from VK, message: %s, reply: %s' %\
                (post, str(reply)))