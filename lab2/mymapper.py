#!/usr/bin/python3
import sys
import re
def remove_punctuation(line):
   punctuation = '''!()-{}[];:"'\,<>./?@#$%^&*_~`+='''
   newline = ''
   for x in line :
      if x not in punctuation:
         newline = newline + x
   return newline


for line in sys.stdin:    
   #line = remove_punctuation(line)
   line = line.strip()  
   words = line.split() 
   for word in words:
      if word.isalpha():
         print (word[0].lower() + "\t" + "1")

      
