#!/usr/bin/python3
import sys

for line in sys.stdin: 
   line = line.strip()
   words = line.split() 

   for word in words:
      print (word + "\t" + "1")
