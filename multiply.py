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
    # key: row_id of the first matrix 'a'
    # value: row vector of 'a' and all column vectors of 'b'
    
    if record[0] == 'a':
        value = [record[0],record[2],record[3]]
        for j in range(5):
            key = (record[1],j) # row_id
            mr.emit_intermediate(key,value)
    else:    
        value = [record[0],record[1],record[3]]
        for j in range(5):
            key = (j,record[2]) # col_id
            mr.emit_intermediate(key,value)
    
# =============================
def reducer(keys, values):
    # keys: entry indices of the matrix C = A*B
    # values: ['a', col_id, value] or ['b', row_id, value]
    a_ij = [0,0,0,0,0]
    b_jk = [0,0,0,0,0]
    for value in values:
        if value[0] == 'a':
            a_ij[value[1]] = value[2]
        else:
            b_jk[value[1]] = value[2]
    c_ik = 0
    for j in range(5):
        c_ik += a_ij[j]*b_jk[j]

    mr.emit((keys[0],keys[1],c_ik))

# =============================
if __name__ == '__main__':
    inputdata = open('./data/matrix.json')
    mr.execute(inputdata, mapper, reducer)

    fileName = raw_input('Please input the file name for output the result:')
    print 'Outputing results ...'
    with open(fileName, 'w') as outfile:
        jenc = json.JSONEncoder()
        outfile.writelines(["%s\n" % jenc.encode(item)  for item in mr.result])

    print 'done.'
