# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Keys Functions Module
"""
# https://www.thepythoncode.com/article/encrypt-decrypt-files-symmetric-python
from cryptography.fernet import Fernet

# Generates a key and save it into keyPath location
def genKey(keyPath):
  key = Fernet.generate_key()
  with open(keyPath, "wb") as key_file:
    key_file.write(key)

# Loads the key stored on keyPath location
def loadKey(keyPath):
  return open(keyPath, "rb").read()

def passEncryptSave(word, key, outPath):
  f = Fernet(key)
  encrypted = f.encrypt(word)
  with open(outPath, "wb") as file:
    file.write(encrypted)

# Given a filename (str) and key (bytes), it decrypts the file and write it
def passDecryptGet(filename, key):
    f = Fernet(key)
    with open(filename, "rb") as file:
      # read the encrypted data
      encrypted_data = file.read()
    # decrypt data
    return f.decrypt(encrypted_data).decode()