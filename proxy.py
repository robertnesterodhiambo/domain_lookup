#!/usr/bin/env python3

import requests

username = "geonode_DrXb2XNsHm-type-residential"
password = "f232262f-0f34-400c-a7a6-84d1ce423302"
GEONODE_DNS = "proxy.geonode.io:9000"
urlToGet = "http://ip-api.com"
proxy = {"http":"http://{}:{}@{}".format(username, password, GEONODE_DNS)}
r = requests.get(urlToGet , proxies=proxy)

print("Response:\n{}".format(r.text))
