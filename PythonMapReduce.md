<center>
	<font size="20" color="blue"><strong>Algorithms in MapReduce: Examples in Python</a></strong></font><br><br>
	<font size="10" color="blue"><font style="font-style:italic;">Chuan Zhang</font> (2014)</font><br><br>
</center>
<hr>

# Synopsis

MapReduce is a programming model and an associated implementation for processing and generating large data sets with a parallel, distributed algorithm on a cluster [<a href="http://en.wikipedia.org/wiki/MapReduce">wikipedia</a>]. It was originally proposed in a 2004 <a href="http://static.googleusercontent.com/media/research.google.com/en/us/archive/mapreduce-osdi04.pdf">paper</a> from a team at Google as a simpler abstraction for processing very large datasets in parallel. In this expository essay, I explain my design of algorithms for solving the six **python mapreduce framework** programming problems from an online course on <a href="http://www.coursera.org">Coursera</a>, <a href="https://class.coursera.org/datasci-002">Introduction to Data Science</a>, provided by Dr. <a href="http://homes.cs.washington.edu/~billhowe/">Bill Howe</a>, from University of Washington.

# Python MapReduce Framework
The goal of this problem set is to give students experience of "**thinking in MapReduce**", Dr. Howe specially designed the framework such that while the framework faithfully implements the MapReduce programming model, it executes entirely on a single machine, and it does not involve parallel computation. In the problems set, a python library called **MapReduce.py** is provided, which implements the MapReduce programming model. The python code below is the provided **MapReduce.py** library.

```python
import json

class MapReduce:
    def __init__(self):
        self.intermediate = {}
        self.result = []

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])
        self.intermediate[key].append(value)

    def emit(self, value):
        self.result.append(value) 

    def execute(self, data, mapper, reducer):
        for line in data:
            record = json.loads(line)
            mapper(record)

        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        jenc = json.JSONEncoder()
        for item in self.result:
            print "%s\n" % jenc.encode(item)
```

# Problem 1
Create an inverted index. Given a set of documents, an inverted index is a dictionary where each word is associated with a list of the documents in which that word appears.

## Mapper Input
The input is a 2 element list: <code>[document_id, text]</code>, where <code>document_id</code> is string is a string representing a document identifier and <code>text</code> is a string representing the text of the document. The document text may have words in upper or lower case and may contain punctuation.

## Reducer Output
The output should be a <code>(word, document_id)</code> tuple where <code>word</code> is a String and <code>document_id</code> is a list of Strings.

## Data set
<pre>./data/books.json</pre>

## My Python Code

```python
import MapReduce
import json

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
```

# Problem 2
Implement a relational join as a MapReduce query. The SQL query is given as follows
```SQL
SELECT * 
FROM Orders, LineItem 
WHERE Order.order_id = LineItem.order_id
```

## Mapper Input

(under construction)
