#!/usr/bin/python
# -*- coding :utf-8 -*-
import jieba
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
	stopwords = stopwords = [line.strip() for line in open('stopwords.txt', 'r', encoding='utf-8').readlines()]

	f1 = open('F:\simulate-hdfs\jieba.txt', 'w', encoding='utf-8')
	for line in sentence:
		# print(line)
		# listsentence.append(line)
		sentence_seged = jieba.cut(line.strip(), cut_all=False)
		for word in sentence_seged:
			if word not in stopwords:
				if word != '\t':
					f1.write(word+' ')
					listorigin.append(word)
		f1.write('\n')


		# f1.write(sentence_seged)
		# print(list(sentence_seged))
		# for word in sentence_seged:
		# 	f1.write(word+' ')
		# 	listorigin.append(word)
			# f1.write(word+' ')
	print(len(listorigin))

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

sentence=sentencesplit('F:\simulate-hdfs\\noteJapan.log')
listword=readsentence(sentence)
print(len(listword))
writevertex('F:\simulate-hdfs\\vertex3.txt',listword)