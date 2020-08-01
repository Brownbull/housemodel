# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  ETL - Extract Transform Load Module
"""
import getpass
from include.keys import * 

keyPath = "D:\Reference\housemodel\config\secrets\postgresql.key"
passPath = "D:\Reference\housemodel\config\secrets\postgresql.pass"
# generate and write a new key
genKey(keyPath)
# load the previously generated key
key = loadKey(keyPath)

passwd = getpass.getpass(prompt="Encrypt:\n").encode()
passEncryptSave(passwd, key, passPath)

## Just for checking
decPass = passDecryptGet(passPath, key)
print("decPass: {}".format(decPass))


