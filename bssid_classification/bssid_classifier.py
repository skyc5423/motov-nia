'''
File: bssid_classifier.py
Project: nia
File Created: 2022-11-26 17:27:38
Author: sangminlee
-----
This script ...
Reference
...
'''

from mongo_helper import MongoHelper
import geohash


class BSSIDClassifier(object):
    PUBLIC_BUS_ID = 0
    MOVING_AP_ID = 1
    NOT_MOVING_AP_ID = 2

    def __init__(self, bssid: str):
        self.bssid = bssid
        self.mh = MongoHelper()
        self.observed_data_list = self.load_observed_data_list()

    def load_observed_data_list(self) -> list:
        return self.mh.query('bssid_collection', {'bssid': self.bssid}).iloc[0]['observed_data_list']

    def classify_by_ssid(self, key_ssid='Public') -> bool:
        ssid = self.observed_data_list[0]['ssid']
        # print('Classify by SSID')
        # print('SSID is %s' % ssid)
        if key_ssid in str(ssid):
            # print('Key SSID (%s) is in the input SSID (%s).' % (key_ssid, ssid))
            return True
        else:
            # print('Key SSID (%s) is not in the input SSID (%s).' % (key_ssid, ssid))
            return False

    def classify_by_gps(self) -> bool:
        # print('Classify by GPS')
        hash_list = []
        for observed_data in self.observed_data_list:
            hash_list.append(geohash.encode(observed_data['latitude'], observed_data['longitude']))
        # print('This AP was observed %d times' % len(self.observed_data_list))
        if len(self.observed_data_list) < 2:
            # print('Cannot classify whether the AP is moving or not.')
            return False
        hash_list.sort()
        if (hash_list[0][:7] == hash_list[-1][:7]) or (hash_list[-1][:7] in geohash.neighbors(hash_list[0][:7])):
            # print('The farthest GPS data between two observations is in neighbors')
            # print('Input BSSID (%s) is not a moving AP' % self.bssid)
            return False
        else:
            # print('The farthest GPS data between two observations is not in neighbors')
            # print('Input BSSID (%s) is a moving AP' % self.bssid)
            return True

    def classify(self) -> int:
        # print('---------------------------------------------------')
        # print("Classifying %s" % self.bssid)
        if self.classify_by_ssid():
            # print('Input BSSID (%s) is Public bus AP' % self.bssid)
            return self.PUBLIC_BUS_ID
        else:
            if self.classify_by_gps():
                return self.MOVING_AP_ID
            else:
                return self.NOT_MOVING_AP_ID
