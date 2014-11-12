import MapReduce
import json

"""
Remove the last 10 characters from each nucleotides,
then remove any duplicates generated.
Chuan Zhang
"""
mr  = MapReduce.MapReduce()

# =============================
def mapper(record):
    # key: sequence_id
    # value: nucleotides
    key = record[0]
    value = record[1]
    sequence = value[:(len(value[0])-11)][:]
    mr.emit_intermediate(sequence,key)
    
# =============================
def reducer(key, list_of_values):
    # key: nucleotide
    # list_of_value: sequence_ids sharing same nucleotide in key
    mr.emit(key)

# =============================
if __name__ == '__main__':
    inputdata = open('./data/dna.json')
    mr.execute(inputdata, mapper, reducer)

    fileName = raw_input('Please input the file name for output the result:')
    print 'Outputing results ...'
    with open(fileName, 'w') as outfile:
        jenc = json.JSONEncoder()
        outfile.writelines(["%s\n" % jenc.encode(item)  for item in mr.result])

    print 'done.'
