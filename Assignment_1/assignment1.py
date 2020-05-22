import apache_beam as beam 
import os
import re

input_file = './data/data.txt'
output_file = './output/output.txt'

def split(element):
    try:
        list_words=element.split()
        list_words_clean = list(map(cleanpunc,list_words))
        list_words_lower = list(map(lambda x:x.lower().strip(),list_words_clean))
        list_word_key = list(map(lambda x:(x,1),list_words_lower))
        return list_word_key
    except:
        pass
    # try:
    #     return element.split()
    # except:
    #     pass

def cleanpunc(sentence): #function to clean the word of any punctuation or special characters
    cleaned = re.sub(r'[?|%|!|+|*|@|&|^|`|~|\'|"|#|=]',r'',sentence)
    cleaned = re.sub(r'[:|;|.|)|(|,|\|/|_|-]',r'',cleaned)
    #cleaned = re.sub(r'\s+',r' ',cleaned)
    return cleaned

p1 = beam.Pipeline()

Word_Count =(

    p1
    |beam.io.ReadFromText(input_file)
    |beam.FlatMap(split)
    |beam.CombinePerKey(sum)
    |beam.io.WriteToText(output_file)
)

p1.run()