#!/usr/bin/python
# -*- coding :utf-8 -*-
# import Defi_func
# import Defi_class
import re
import numpy as np
import jieba
from operator import add
from pyspark import SparkContext


#将评论分为句子
def sentencesplit(file):
	f1=open(file,'r',encoding='utf-8')
	sentence=[]
	for line in f1:
		result=line.split('。')
		for num in range(len(result)):
			sentence.append(result[num])
			# print(result)
	return sentence

#分词并删除重复的
def readsentence(sentence):
	# f1=open(file,'r',encoding='utf-8')
	listword=[]
	listorigin=[]
	for line in sentence:
		# print(line)
		# listsentence.append(line)
		sentence_seged = jieba.cut(line.strip(), cut_all=False)
		# print(list(sentence_seged))
		for word in sentence_seged:
			listorigin.append(word)
	# print(len(listorigin))

	for i in listorigin:
		if i not in listword:
			listword.append(i)
	# listword = list(set(listorigin))
	# print(listword)
	# print(len(listword))
	return listword

#将分词写入vertex.txt
def writevertex(file,listword):
	f1=open(file,'w',encoding='utf-8')
	num=0
	for word in listword:
		f1.write(str(num))
		f1.write(' ')
		f1.write(word)
		f1.write('\n')
		num=num+1


#算边的权重
def writeedge(listword,listsentence,file1,file2):
	f1=open(file1,'w',encoding='utf-8')
	f2=open(file2,'w',encoding='utf-8')
	lenword=len(listword)
	newword=[]
	newword2=[]
	for i in range(len(listword)):

		for j in range(len(listword)):
			num = 0
			for k in range(len(listsentence)):
				if listword[i]+listword[j] in listsentence[k]:
					num=num+1
			if num>1:
				if i !=j:
					f1.write(str(i) + ' ' + str(j) + ' ' + str(num))
					f1.write('\n')
					f2.write(str(i) + ' ' + listword[i])
					f2.write('\n')
					f2.write(str(j) + ' ' + listword[j])
					f2.write('\n')
					newword.append(listword[i])
					# newword.append(listword[j])
					print(i, j, num)


				
	for i in newword:
		if i not in newword2:
			newword2.append(i)
	return newword2

if __name__ == "__main__":
	array=[]

	# print(stopwords)
	f1=open('F:\simulate-hdfs\\vertex3.txt','w',encoding='utf-8')
	sc = SparkContext("local",appName="PythonWordCount")
	lines = sc.textFile('F:\simulate-hdfs\jieba.txt')
	counts = lines.flatMap(lambda x: x.split(' ')) \
		.map(lambda x: (x, 1)) \
		.reduceByKey(add)
	counts.collect()
	result=counts.filter(lambda x:x[1]>20)
	output = result.collect()
	for (word, count) in output:
		array.append(word)
		print("%s: %i" % (word, count))
		f1.write(word + ' ' + str(count) + '\n')


	print(len(array))
	sc.stop()
	sentence=sentencesplit('F:\simulate-hdfs\\noteJapan.log')
	# listword=readsentence(sentence)
	# print(len(listword))
	writevertex('F:\simulate-hdfs\\vertex.txt',array)
	newword=writeedge(array,sentence,'F:\simulate-hdfs\\edge.txt','F:\simulate-hdfs\\vertex2.txt')
	print(len(newword))
	writevertex('F:\simulate-hdfs\\vertex.txt',newword)
	writeedge(newword,sentence,'F:\simulate-hdfs\\edge.txt','F:\simulate-hdfs\\vertex2.txt')
	# print(sentence)
	# print(len(sentence))
	# print(listsentence)
	# print(len(listsentence))
	# computeredge(listword,sentence)




