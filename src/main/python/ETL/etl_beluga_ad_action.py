# Firehose 에서 들어오는 beluga-ad-action log 를 변환하는 lambda function.
# 1. decrypt bidPrice
# 2. Flatten

from __future__ import print_function

import base64
import json
import datetime
import sys
import binascii

from Crypto.Protocol.KDF import PBKDF2
from Crypto.Cipher import AES
from Crypto import Random

print('Loading function')


def lambda_handler(event, context):
  output = []

  for record in event['records']:
      data = base64.b64decode(record['data']) 
      json_data = json.loads(data)
      print('json data: {}'.format(json_data))
      json_data['content']['bidPrice'] = decrypt(json_data['content']['bidPrice'], dec_pw, dec_salt)
      new_data = flatten_json(json_data)
  
      output_record = {
          'recordId': record['recordId'],
          'result': 'Ok',
          'data': base64.b64encode(json.dumps(new_data, separators=(',', ':')).encode('utf-8') + b'\n').decode('utf-8')
      }
      
      output.append(output_record)

  print('Successfully processed {} records.'.format(len(event['records'])))
  return {'records': output}
    

# Function for flattening json 
def flatten_json(y): 
  out = {} 

  def flatten(x, name =''): 
        
      # If the Nested key-value  
      # pair is of dict type 
      if type(x) is dict: 
            
          for a in x: 
              flatten(x[a], a + '_') 
                
      # If the Nested key-value 
      # pair is of list type 
      elif type(x) is list: 
            
          i = 0
            
          for a in x:                 
              flatten(a, str(i) + '_') 
              i += 1
      else: 
          out[name[:-1]] = x if x != '<null>' else None

  flatten(y) 
  return out

class AesCrypt256:
    # Based on https://gist.github.com/pfote/5099161
    BLOCK_SIZE = 16
    # To use the null/x00 byte array for the IV
    default_initialization_vector = False

    def __init__(self, default_initialization_vector=False):
        self.default_initialization_vector = default_initialization_vector

    def pkcs5_pad(self, s):
        return s + (self.BLOCK_SIZE - len(s) % self.BLOCK_SIZE) * chr(self.BLOCK_SIZE - len(s) % self.BLOCK_SIZE)

    def pkcs5_unpad(self, s):
        # from https://jhafranco.com/2012/01/16/aes-implementation-in-python/
        return "".join(chr(e) for e in s[:-s[-1]])

    def _encrypt(self, key, value, iv):
        cipher = AES.new(key, AES.MODE_CBC, iv)
        crypted = cipher.encrypt(self.pkcs5_pad(value).encode('utf-8'))
        # check if empty/null initialization vector, and do not prepend if null
        if all(v == 0 for v in iv):
            return crypted
        else:
            # prepend the initialization vector
            return iv + crypted

    def _decrypt(self, key, value, iv):
        cipher = AES.new(key, AES.MODE_CBC, iv)
        # unpad the bytes, throw away garbage at end
        return self.pkcs5_unpad(cipher.decrypt(value))

    def encrypt(self, key, value):
        if self.default_initialization_vector:
            return self._encrypt(key, value, bytes(bytearray(16)))
        else:
            iv = Random.get_random_bytes(16)
            return self._encrypt(key, value, iv)

    def decrypt(self, key, value):
        if self.default_initialization_vector:
            # we do not have an IV present
            default_iv = bytes(bytearray(16))
            return self._decrypt(key, value, default_iv)
        else:
            iv = value[:16]
            crypted = value[16:]
            return self._decrypt(key, crypted, iv)

    def encryptHex(self, key, value):
        return binascii.hexlify(self.encrypt(key, value))

    def decryptHex(self, key, value):
        return self.decrypt(key, binascii.unhexlify(value))

# 환경 변수로 옮겨야함
dec_pw = "7dd21f4c-1c37-4688-9acc-31423e0a523d"
dec_salt = "6da71b2bd1789727"

def decrypt(encry_data, password, salt):
    key = PBKDF2(password=password, salt=binascii.unhexlify(salt), dkLen=32, count=1024)
    encryptor = AesCrypt256(default_initialization_vector=False)
    try:
        return encryptor.decryptHex(key, encry_data)
    except Exception:
        return encry_data

if __name__ == '__main__':
  event = {
    "invocationId": "invoked123",
    "deliveryStreamArn": "aws:lambda:events",
    "region": "us-west-2",
    "records": [
      {
        "data": "eyJtZXRhIjogeyJ0aW1lc3RhbXAiOiAiMTY2MDcxNTAxNjI3NCIsICJkYXRldGltZSI6ICIyMDIyLTA4LTE3VDE0OjQzOjM2KzA5MDAiLCAidXNlclR5cGUiOiAiUiIsICJ1c2VyaWQiOiAidG85YXoiLCAic3RvcmVJZCI6ICIzMjQ2ODkiLCAicGxhdGZvcm0iOiAiSU9TIiwgIm9zVmVyc2lvbiI6ICIxNS42IiwgImFwcFZlcnNpb24iOiAiNC4zNy4xIiwgInV1aWQiOiAiRjc4RDI3N0ZDQjdCNDBCMEE2OEZEMTkwMzU1MTc4RTIiLCAic2NyZWVuTmFtZSI6ICLshozrp6Rf7IOB7ZKI66qp66GdX+yghOyytOyLoOyDgSIsICJzY3JlZW5MYWJlbCI6ICJUVE4iLCAicmVmZXJyZXIiOiAi7IaM66ekX+yDge2SiOuqqeuhnV/rnq3tgrkiLCAicmVmZXJyZXJMYWJlbCI6IG51bGwsICJldmVudCI6ICJJTVAifSwgImNvbnRlbnQiOiB7Imdyb3VwSWR4IjogIjg5MDEiLCAiY2FtcGFpZ25JZHgiOiAiODkwNyIsICJwYWdlSWR4IjogIjEyIiwgInNlbGVjdGlvbklkIjogImVjNTM0ZDEwLWQ2YTEtNDBlMy05MWQxLTEwMmQ0ZGJhYmJjZCIsICJzZWxlY3Rpb25Hcm91cElkIjogImYyYTUyZDdhLTMxZjEtNDM4Ny1iMWFkLTBlZTZlZWY0NzRlMCIsICJjaGFyZ2luZ1R5cGUiOiAiQ1BDIiwgInNlbGVjdGlvblRpbWUiOiAiMjAyMi0wOC0xN1QxNDozOTowMS4xNjkrMDk6MDAiLCAiYmlkUHJpY2UiOiAiMzU4NjAxMDgxYzgxM2RiMDhiOWUxMjQwZmUyZGM5YTcxNzMwMTMyZThhNjg3Yzg3NTIyMDEyMDYzYzE4OWMxOCIsICJjcmVhdGl2ZUlkeCI6ICI3NzU4MjYiLCAicHJvZHVjdElkeCI6ICI0IiwgImFjY291bnRJZCI6ICI1ZDY5YTk3NS0zMzFhLTRiYjktODFiNS0yMTY4YWJkOTA5YTgiLCAid3NJZHgiOiAiMjY3MjkiLCAiY2RJZHgiOiAiPG51bGw+IiwgInVuaXRJZHgiOiAiMTU2IiwgInF1ZXJ5IjogIjxudWxsPiIsICJyc0lkeCI6ICIzMjQ2ODkiLCAia2V5d29yZElkeCI6ICI8bnVsbD4iLCAia2V5d29yZCI6ICI8bnVsbD4iLCAiZXhwb3N1cmVUaW1lIjogIjE2NjA3MTUwMTYyNzMifX0K",
        "recordId": "record1",
        "approximateArrivalTimestamp": 1510772160000,
        "kinesisRecordMetadata": {
          "shardId": "shardId-000000000000",
          "partitionKey": "4d1ad2b9-24f8-4b9d-a088-76e9947c317a",
          "approximateArrivalTimestamp": "2012-04-23T18:25:43.511Z",
          "sequenceNumber": "49546986683135544286507457936321625675700192471156785154",
          "subsequenceNumber": ""
        }
      },
      {
        "data": "eyJtZXRhIjogeyJ0aW1lc3RhbXAiOiAiMTY2MDcxNTAxNzQ5MCIsICJkYXRldGltZSI6ICIyMDIyLTA4LTE3VDE0OjQzOjM3KzA5MDAiLCAidXNlclR5cGUiOiAiUiIsICJ1c2VyaWQiOiAic2NvczE1IiwgInN0b3JlSWQiOiAiMTQ4NDkiLCAicGxhdGZvcm0iOiAiQU5EUk9JRCIsICJvc1ZlcnNpb24iOiAiMTIiLCAiYXBwVmVyc2lvbiI6ICI0LjM3LjIiLCAidXVpZCI6ICI3YjA0MWIxZC1hNjFmLTNjZjItYTkzZC1iZmRlMzU3ODkzNDYiLCAic2NyZWVuTmFtZSI6ICLshozrp6Rf7IOB7ZKI7IOB7IS4X+ydvOuwmCIsICJzY3JlZW5MYWJlbCI6ICJHRFQiLCAicmVmZXJyZXIiOiAi7IaM66ekX+uPhOunpO2ZiF/rqZTsnbgiLCAicmVmZXJyZXJMYWJlbCI6ICJXU0giLCAiZXZlbnQiOiAiSU1QIn0sICJjb250ZW50IjogeyJwYWdlSWR4IjogIjgiLCAic2VsZWN0aW9uSWQiOiAiNDI0YWJjNjgtODA5Ni00MDRmLWJkMWYtMzMwNjAzMjM1ZmVkIiwgInNlbGVjdGlvbkdyb3VwSWQiOiAiY2UyNTExY2EtODgxYy00N2I4LWIzZDktYzg2ODhjMDJjYzliIiwgImNoYXJnaW5nVHlwZSI6ICJDUEMiLCAic2VsZWN0aW9uVGltZSI6ICIyMDIyLTA4LTE3VDE0OjQzOjM2LjIyKzA5OjAwIiwgImJpZFByaWNlIjogIjk5YmFiZjkxODY5OTdhMTcyNjYwOGVmOGYxMTcyZmNlYzQyZGQ3N2NhYTcyM2Y0MjhlZDdkY2VhNzZlMTcxY2UiLCAiY3JlYXRpdmVJZHgiOiAiNzc1NDAyIiwgImdyb3VwSWR4IjogIjMzNDIiLCAiY2FtcGFpZ25JZHgiOiAiMzM0MiIsICJwcm9kdWN0SWR4IjogIjIiLCAiYWNjb3VudElkIjogIjhhZjYxOGYyLWEzNTQtNDU3ZC05M2NkLWZhOWE0NzczZTMyMCIsICJ3c0lkeCI6ICIyMDM3NyIsICJjZElkeCI6ICIyIiwgInVuaXRJZHgiOiAiMTAxIiwgInF1ZXJ5IjogbnVsbCwgInJzSWR4IjogIjE0ODQ5IiwgImtleXdvcmRJZHgiOiAiNjk1NjA3MCIsICJrZXl3b3JkIjogIu2LsCIsICJleHBvc3VyZVRpbWUiOiAiMTY2MDcxNTAxNzQ4OSJ9fQo=",
        "recordId": "record2",
        "approximateArrivalTimestamp": 151077216000,
        "kinesisRecordMetadata": {
          "shardId": "shardId-000000000001",
          "partitionKey": "4d1ad2b9-24f8-4b9d-a088-76e9947c318a",
          "approximateArrivalTimestamp": "2012-04-23T19:25:43.511Z",
          "sequenceNumber": "49546986683135544286507457936321625675700192471156785155",
          "subsequenceNumber": ""
        }
      }
    ] 
  }

  # print('[DEBUG] event:\n{}'.format(event), file=sys.stderr)
  output = lambda_handler(event, {})
  print('output:\n{}'.format(output), file=sys.stderr)
  
