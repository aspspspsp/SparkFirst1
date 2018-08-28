import os
root = "/home/hadoop/pys/PDFReorganization"

paths = [x[0] for x in os.walk(root)]

for path in paths:
     files = os.listdir(path)
     if len(files) == 0:
         os.rmdir(path)
         print(path)