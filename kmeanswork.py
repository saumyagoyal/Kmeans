from __future__ import print_function
import sys
from pyspark import SparkContext
import numpy as np
import random
import csv
import binascii
from bisect import bisect_right
import re

#hadoop fs -put /home/sg5290/Articles.csv /user/sg5290/Articles.csv
#--------Reads the list
DocumentList=[]
with open("/home/sg5290/Articles.csv","rU") as csvfile:
#with open("/Users/Saumya/Documents/Article.csv","rU") as csvfile:
   spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
   for row in spamreader:
		print("---->")
		#print ''.join(row[0])
		DocumentList.append(''.join(row[0]))
		print("<----")


Document=sc.parallelize(DocumentList)

def shingles(k, lines):
	char=re.sub("\s\s+"," ",lines)
	characters = str(char).replace("\\s+", " ").replace("(","").replace(")","").replace("[", "").replace("]", "").replace("\\r", "").replace("\\n", "").replace("\\", "").replace(",","")
	slist = set()
	for i in range (0, len(characters)):
		#value = str(i+1)+","
		value = ""
		for j in range (0, k) :
			if(len(characters) > (i+j)):
				value = value + characters[i+j]
			else :
				break
		#slist.append(value) 
		slist.add(value)
	return slist


numberOfHashFunctions = 100
bigPrime= 4294967311
minHashCode=bigPrime+1
numberOfShingles=500

a=[]
b=[]
for i in range (0, numberOfHashFunctions) :
	a.append(random.randint(1,numberOfShingles-1))
	b.append(random.randint(1,numberOfShingles-1))


numberDocument=Document.count()
#numberDocument=100
#2692

oneDoc=Document.zipWithIndex().map(lambda (x, y): (y,x))
shingleList=[]

for d in range (1, numberDocument):
	docum=oneDoc.lookup(d)
	doc1=sc.parallelize(docum)
	#doc1=Document[d]
	#shingTemp=shingles(5,doc1)
	shingTemp=doc1.flatMap(lambda x :shingles(5,x))
	#shingleList[d]=shingTemp.zipWithIndex().map(lambda (x, y): (y,x))
	shingleList.append(shingTemp.zipWithIndex().map(lambda (x, y): (y,x)))	


for d in range (0, numberDocument-1):	
	numberOfShingles = shingleList[d].count()
	print("Number of Shingles",numberOfShingles)


# ----> minHash the shingles for one hasgFunction and take the min. Repeat for 100 such hashfunctions
hashVectors =[]
minHashAllDocuments = []
minHashPerDocument = []
for j in range (1, numberDocument) :
	hashVectors =[]
	for i in range(0, numberOfHashFunctions) :
		hashV=900
		minHashCode=bigPrime+1
		hashV=shingleList[j-1].map(lambda (x,y) : (((a[i]*(binascii.crc32(str(y)) & 0xffffffff)) +b[i])% bigPrime))
		hashVector=hashV.min()
		hashVectors.append(hashVector)
		#print(hashVectors)
	minHashAllDocuments.append(hashVectors)
	print(minHashAllDocuments[j-1])


sc.parallelize(minHashAllDocuments).saveAsTextFile("minHashOutput100.txt")

#sc.parallelize(minHashAllDocuments).saveAsTextFile("/Users/Saumya/Documents/minHashOutputSmall.txt")

# ---------------K means
from __future__ import print_function
import sys
import numpy as np
from pyspark.sql import SparkSession


# lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
# data = lines.map(parseVector).cache()

K = 4
convergeDist = 0.1
allMinHash=sc.parallelize(minHashAllDocuments)

# kPoints = data.takeSample(False, K, 1)
tempDist = 1.0
allMinHashZip=allMinHash.zipWithIndex().map(lambda (x, y): (y,x))
allMinHashZip.count()
allMinHashZip.lookup(0)

def distanceSquared(l1,l2):
	total=len(l1)
	temp=0
	for i in range(total):
		temp+=(l1[i]-l2[i])**2
	return temp**(1.0/2.0)


#distanceSquared(allMinHashZip.lookup(0)[0],centroidPoints[0])
def closestPoint(p, centroidPoints):
    bestIndex = -1
    closest = sys.maxint
    for i in range(len(centroidPoints)):
    	tempDist=distanceSquared(p,centroidPoints[i])
        #tempDist = np.sum((p - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex


def divide(l,value):	
	temp=[]
	for i in range (0,len(l)):
		temp.append(l[i]/value)
	return temp


def addition(l1,l2):
	total=[]
	for i in range(0,len(l1)):
		total.append(l1[i]+l2[i])
	return total


centroidPoints=allMinHash.takeSample(False, K, 1)
for i in range (0, 10):
	closest = allMinHash.map(lambda p: (closestPoint(p, centroidPoints), (p, 1)))
	#closest.collect()
	pointStats = closest.reduceByKey(lambda p1_c1, p2_c2: (addition(p1_c1[0], p2_c2[0]), p1_c1[1] + p2_c2[1]))
	#pointStats.collect()
	newPoints = pointStats.map(lambda st: (st[0], divide(st[1][0], st[1][1]))).collect()
	#newPoints.collect()
	for (iK, p) in newPoints:
		centroidPoints[iK] = p


pos=[]
for i in range (0, allMinHashZip.count()) :
	dis=0
	min=sys.maxint
	cluster=-1
	for j in range (0, len(centroidPoints)):
		dis=distanceSquared(allMinHashZip.lookup(i)[0],centroidPoints[j])
		if dis<min:
			min=dis
			cluster=j
	print("Documents- ", i," in cluster ",cluster)


























