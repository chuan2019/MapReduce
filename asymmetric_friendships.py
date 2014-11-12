import MapReduce
import json

"""
Finding mutual friends
Chuan Zhang
"""
mr  = MapReduce.MapReduce()

# =============================
def mapper(record):
    # key: person name
    # value: friend names
    key = record[0]
    value = [record[0],record[1]]
    mr.emit_intermediate(key,value)
    key = record[1]
    mr.emit_intermediate(key,value)
    
# =============================
def reducer(key, list_of_values):
    # key: person name
    # list_of_value: friends of key
    for value in list_of_values:
        value_swapped = [value[1],value[0]]
        if value_swapped not in list_of_values:
            if value[0] == key:
                mr.emit((value[0],value[1]))
            else:
                mr.emit((value[1],value[0]))

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

