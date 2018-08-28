# this python scrip can upload file to hdfs server
from hdfs import InsecureClient

upload_path = '/vectors'
file_path = 'C:\\word_vector_sh.vec'

client = InsecureClient('http://master32:50070', user='hadoop')
client.upload(upload_path,
               file_path,
               overwrite=False,
               n_threads=0,
               temp_dir=None,
               chunk_size=65536,
               progress=None,
               cleanup=True)

with client.read(upload_path, chunk_size=8096) as reader:
    for chunk in reader:
        print chunk