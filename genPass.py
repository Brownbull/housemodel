# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Gen Pass - generates key and stores encrypted passwords
"""
import getpass
from include.keys import * 

keyPath = "D:\Reference\housemodel\config\secrets\key.key"
passPath = "D:\Reference\housemodel\config\secrets\postgresql.pass"
# generate and write a new key
genKey(keyPath)
# load the previously generated key
key = loadKey(keyPath)

passwd = getpass.getpass(prompt="Encrypt:\n").encode()
passEncryptSave(passwd, key, passPath)

## Just for checking
# decPass = passDecryptGet(passPath, key)
# print("decPass: {}".format(decPass))


