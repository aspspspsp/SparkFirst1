# import hdfs library to Python
from hdfs import InsecureClient

# log in hdfs server
client = InsecureClient('http://master32:50070', user='hadoop')

# print all of the hdfs root folder
print client.list('/')

path = '/test/aaa.txt'

# Check if the file exists
if (client.content(path, strict=False) != None):
    client.delete(path)

print "START TO WRITE FILE"

# write a text file from hdfs
with client.write(path, encoding='utf-8') as writer:
    for i in range(10):
        writer.write("Hello World\n")

print "DONE"

print "START TO READ FILE"

# read a text file from hdfs
with client.read(path, chunk_size=8096) as reader:
    for chunk in reader:
        print chunk

print "DONE"