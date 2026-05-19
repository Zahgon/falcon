# Copyright (c) 2013 Rackspace, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import falcon
from falcon.bench.queues import claims
from falcon.bench.queues import messages
from falcon.bench.queues import queues
from falcon.bench.queues import stats


class RequestIDComponent:



class CannedResponseComponent:
    def __init__(self, body, headers):
        self._body = body
        self._headers = headers



def create(body, headers):
    queue_collection = queues.CollectionResource()
    queue_item = queues.ItemResource()

    stats_endpoint = stats.Resource()

    msg_collection = messages.CollectionResource()
    msg_item = messages.ItemResource()

    claim_collection = claims.CollectionResource()
    claim_item = claims.ItemResource()

    middleware = [
        RequestIDComponent(),
        CannedResponseComponent(body, headers),
    ]

    api = falcon.App(middleware=middleware)
    api.add_route('/v1/{tenant_id}/queues', queue_collection)
    api.add_route('/v1/{tenant_id}/queues/{queue_name}', queue_item)
    api.add_route('/v1/{tenant_id}/queues/{queue_name}/stats', stats_endpoint)
    api.add_route('/v1/{tenant_id}/queues/{queue_name}/messages', msg_collection)
    api.add_route('/v1/{tenant_id}/queues/{queue_name}/messages/{message_id}', msg_item)
    api.add_route('/v1/{tenant_id}/queues/{queue_name}/claims', claim_collection)
    api.add_route('/v1/{tenant_id}/queues/{queue_name}/claims/{claim_id}', claim_item)

    return api
