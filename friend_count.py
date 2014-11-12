import MapReduce
import sys
import json

"""
Counting friends
Chuan Zhang
"""
mr  = MapReduce.MapReduce()

# =============================
def mapper(record):
    # key: person name
    # value: friend names
    key = record[0]
    value = record[1]
    mr.emit_intermediate(key,value)

# =============================
def reducer(key, list_of_values):
    # key: person name
    # list_of_value: list of names of friends
    value = len(list_of_values)
    mr.emit((key,value))

# =============================
if __name__ == '__main__':
    inputdata = open('./data/friends.json')
    mr.execute(inputdata, mapper, reducer)

    fileName = raw_input('Please input the file name for output the result:')
    print 'Outputing results ...'
    with open(fileName, 'w') as outfile:
        jenc = json.JSONEncoder()
        outfile.writelines(["%s\n" % jenc.encode(item)  for item in mr.result])

    print 'done.'

