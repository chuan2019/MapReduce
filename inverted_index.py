import MapReduce
import json

"""
Given a set of documents, an inverted index is a
dictionary where each word is associated with a list
of the document identifiers in which that word appears.
Chuan Zhang
"""

mr = MapReduce.MapReduce()
# =============================
def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, key)

# =============================
def reducer(key, list_of_values):
    # key: word
    # value: list of document_ids
    list_of_values = list(set(list_of_values))
    mr.emit((key, list_of_values))

# =============================
if __name__ == '__main__':
    inputdata = open('./data/books.json')
    mr.execute(inputdata, mapper, reducer)

    fileName = raw_input('Please input the file name for output the result:')
    print 'Outputing results ...'
    with open(fileName, 'w') as outfile:
        jenc = json.JSONEncoder()
        outfile.writelines(["%s\n" % jenc.encode(item)  for item in mr.result])

    print 'done.'

