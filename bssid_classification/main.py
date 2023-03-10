'''
File: main.py
Project: nia
File Created: 2022-10-07 16:12:45
Author: sangminlee
-----
This script ...
Reference
...
'''
from mongo_helper import MongoHelper
from bssid_classifier import BSSIDClassifier
from tqdm import tqdm


def classify_bssid():
    mh = MongoHelper()
    collection = mh.query_key('bssid_collections', key_list=['bssid'])
    for i in tqdm(range(collection.shape[0])):
        bssid = collection.iloc[i]['bssid']
        classifier = BSSIDClassifier(bssid)
        ap_class = classifier.classify()
        mh.update_collection('bssid_collections', {'bssid': bssid}, {'ap_class': ap_class})


def main():
    classify_bssid()


if __name__ == '__main__':
    main()
