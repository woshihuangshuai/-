#!/usr/bin/env python
# -*-coding:utf-8 -*-

from pymongo import MongoClient
import os
import os.path
import csv
import collections
import time

key = []
key.append('消息ID'.decode('utf-8'))
key.append('用户ID'.decode('utf-8'))
key.append('用户名'.decode('utf-8'))
key.append('屏幕名'.decode('utf-8'))
key.append('转发消息ID'.decode('utf-8'))
key.append('消息内容'.decode('utf-8'))
key.append('消息URL'.decode('utf-8'))
key.append('来源'.decode('utf-8'))
key.append('图片URL'.decode('utf-8'))
key.append('音频URL'.decode('utf-8'))
key.append('视频URL'.decode('utf-8'))
key.append('地理坐标'.decode('utf-8'))
key.append('转发数'.decode('utf-8'))
key.append('评论数'.decode('utf-8'))
key.append('赞数'.decode('utf-8'))
key.append('发布时间'.decode('utf-8'))

root_dir = 'I:\Users\HS\Desktop\sina_data'

client = MongoClient('localhost', 27017)
db = client['sina_data']
collection = db['weibo_data2']

failed = 0

for parent, subdirs, filenames in os.walk(root_dir):
    for filename in filenames:
        path = parent + '\\' + filename
        print 'filepath: ' + path
        
        reader = csv.reader(file(path, 'r'))      
        try:
            for line in reader:
                if reader.line_num == 1:
                    continue

                doc = collections.OrderedDict()
                for i in xrange(0, 16):
                    try:
                        '''if i == 15:
                            t = time.localtime(int(line[15])/1000)
                            print t
                            time_str = time.strftime('%Y-%m-%d %H:%M:%S', t)
                            print time_str
                            doc[key[i]] = time_str
                        else:'''
                        doc[key[i]] = line[i].decode('utf-8')
                    except Exception, e:
                        continue

                collection.insert(doc)

        except Exception, e:
            failed += 1
            continue
                
print 'Failed: ', failed

raw_input()