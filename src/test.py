#!/usr/bin/env python

import os
datei = '20150101'
for para in range(10):
    cmd = "PATH/tf_idf.py {datei}".format(datei=datei)
    print cmd
    status = os.system(cmd)
    if status != 0:
        raise("error")
