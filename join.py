import MapReduce
import json

"""
Join two "tables"
Chuan Zhang
"""
mr  = MapReduce.MapReduce()
# =============================
def mapper(record):
    # key: order identifier
    # value: attributes of each record
    key = record[1]
    value = record
    mr.emit_intermediate(key, value)

# =============================
def reducer(key, list_of_values):
    # key: order identifier
    # list_of_value: record attributes (orders and line_items)
    
    for items in list_of_values:
        value = list_of_values[0][:]
        if items[0] != 'order':
            value += items
            mr.emit(value)

# =============================
if __name__ == '__main__':
    inputdata = open('./data/records.json')
    mr.execute(inputdata, mapper, reducer)

    fileName = raw_input('Please input the file name for output the result:')
    print 'Outputing results ...'
    with open(fileName, 'w') as outfile:
        jenc = json.JSONEncoder()
        outfile.writelines(["%s\n" % jenc.encode(item)  for item in mr.result])

    print 'done.'

