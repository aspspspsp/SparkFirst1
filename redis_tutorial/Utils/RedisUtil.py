#!/usr/bin/python
#-*-coding:utf-8 -*-

from rediscluster import StrictRedisCluster
import sys

def GetRedisCluster():
    redis_nodes = [{'host': '192.168.4.1', 'port': 7001},
                   {'host': '192.168.4.2', 'port': 7002},
                   {'host': '192.168.4.3', 'port': 7003},
                   {'host': '192.168.4.4', 'port': 7004},
                   {'host': '192.168.4.5', 'port': 7005},
                   {'host': '192.168.4.6', 'port': 7006}]

    try:
        redisconn = StrictRedisCluster(startup_nodes=redis_nodes)
    except Exception, e:
        print "connect error"
        sys.exit(1)

    return redisconn