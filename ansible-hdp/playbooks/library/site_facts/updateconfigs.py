#!/usr/bin/env python

import hashlib
import json
import re
import six
import sys
from ambariclient.client import Ambari
from ansible.module_utils.basic import *
from datetime import datetime

GB = 1024


def update_config(cluster, config_name, new_properties):
    """Update configuration for an Ambari service"""
    tag = max((config.version, config.tag) for config in cluster.configurations(type=config_name))[-1]

    try:
        config = six.next(cluster.refresh().configurations(type=config_name, tag=tag))
    except StopIteration:
        print 'No configuration found for config {} at tag {}'.format(config_name, tag)
        sys.exit(1)

    properties = config.properties
    original_sha = hashlib.sha256(json.dumps(properties)).hexdigest()

    """Sanitise new properties"""


for key in new_properties.iterkeys():

    new_conf = dict()

    for key in new_properties.iterkeys():

        property = key.replace('_', '.', 10).replace('-', '.')
        re_obj = re.compile(property)
        for my_key in properties:
            my_key = str(my_key)
            if re.match(re_obj, my_key):
                property = new_properties[key]
                new_conf[my_key] = property

    properties.update(new_conf)
    new_sha = hashlib.sha256(json.dumps(properties)).hexdigest()

    if original_sha == new_sha:
        print "Nothing to update"
        sys.exit(0)

    timestamp = int((datetime.now() - datetime.fromtimestamp(0)).total_seconds()) * 1000
    new_version = 'version{}'.format(timestamp)

    data = {'desired_config': {
        'type': config_name,
        'tag': new_version,
        'properties': properties
    }
    }
    cluster.update(Clusters=data)


def main():
    module = None

    module = AnsibleModule(
        argument_spec=dict(
            ambari_server=dict(default='localhost', type='str'),
            ambari_pass=dict(default='admin', type='str'),
            cluster_name=dict(default='hadoop-poc', type='str'),
            config_name=dict(type='str'),
            properties=dict(type='dict')
        )
    )

    ambari_server = module.params.get('ambari_server')
    ambari_pass = module.params.get('ambari_pass')
    cluster_name = module.params.get('cluster_name')
    config_name = module.params.get('config_name')
    properties = module.params.get('properties')

    client = Ambari(ambari_server, port=8080, username=ambari_user, password=ambari_pass)

    update_config(next(client.clusters), config_name, properties)

    module.exit_json(changed=True,
                     ansible_facts=dict())


if __name__ == '__main__':
    main()
