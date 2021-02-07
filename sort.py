from _thread import *
import heapq
import os
import time
import sys
import math
import threading
from threading import Thread
from threading import *



def colname_to_colnum(col_names):
	col_number=[]
	for name in col_names:
		line_number=0
		with open("metadata.txt",'r') as abc:
			for line in abc:
				if line.startswith(name):
					col_number.append(line_number)
					break
				line_number=line_number+1
	return col_number


def createInitialRuns(input_file,tuplesinchunk,no_of_rows):
 
	input_content = open(input_file,"r") 
	content= input_content.readlines()

	i=0
	filename=int(1)
	filesize=tuplesinchunk

	while(i<=no_of_rows and i+filesize<=no_of_rows):
		output_content=content[i:i+filesize]
		f = open(str(filename)+".txt", "w")
		for x in output_content:
			f.write(x)
		f.close()
		filename=str(int(filename)+1)
		i=i+filesize
	
	if i<no_of_rows:
		output_content=content[i:]
		f = open(str(filename)+".txt", "w")
		for x in output_content:
			f.write(x)
		f.close()

	return filename 		

def sort_each_file(filename,sort_by,col_numbers):
	input_content = open(filename,"r") 
	content1= input_content.readlines()
	content=[]
	for x in content1:
		x=x.split("  ")
		x[-1]=x[-1].strip()
		content.append(x)
		i=0
	#print(content)
	col_numbers.reverse()
	for index in col_numbers:
		content.sort(key=lambda content:content[index+i]) 
		for x in content:
			x.insert(0,x[index+i])
		i=i+1

	if sort_by=="asc":
		pass
	elif sort_by=="desc":
		content.reverse()
	

	f=open(filename, "w")
	print("writing to file:")
	for x in content:
		aa=""
		for x1 in x:
			aa=aa+x1+" "
		f.write(aa.rstrip()+"\n")
	f.close()
	

def mergeFiles(output_file, no_of_file,sort_by,col_size):
	fp=[0]
	f=open(output_file,"w")
	heap=[]
	d=dict() # to store the whole role
	for i in range(1,int(no_of_file)+1) :
		ff=str(str(i)+".txt")
		chunks=open(ff,"r")
		temp=chunks.readline()
		if(temp==""):
			continue
		d[i]=temp
		temp=temp.strip("\n").split(" ")
		if(temp[-1]==""):
			temp=temp[:-1]
		key=""
		for pp in range(col_size):
			key=key+temp[pp] 
		heap.append((key,i))
		fp.append(chunks)

	if(sort_by.lower()=="asc"):
		heapq.heapify(heap)
	else:
		heapq._heapify_max(heap)

	while(len(heap)>0):
		if(sort_by.lower()=="asc"):
			temp=heapq.heappop(heap)
		else:
			temp=heapq._heappop_max(heap)
		
		key,index=temp[0],temp[1]
		next_file=fp[index]
		next=next_file.readline()
		fp[index]=next_file
		a=d[index].split(" ")
		aainfile=""
		for aa in a[col_size:]:
			aainfile+= aa +" "
		f.write(aainfile.rstrip()+"\n")
		d[index]=next
		next=next.strip("\n").split(" ")
		if(next[-1]==""):
			next=next[:-1]
		if(len(next)>0):
			key=""
			key=key+temp[0] 
			heapq.heappush(heap,(key,index))
			if(sort_by.lower()=="desc"):
				heapq._heapify_max(heap)
	f.close()
	
	for i in range(1,int(no_of_file)):
		os.remove(str(i) +".txt")
	return


class chunk_sort_threading(Thread):
	def __init__(self,i,sort_by,col_number):
		super(chunk_sort_threading,self).__init__()
		self.i=i
		self.sort_by=sort_by
		self.col_number=col_number
		self.start()
	def run(self):
		tl.acquire()
		try:
			print("Sorting :"+str(self.i)+" subfile")
			filename=str(self.i)+".txt"
			input_content = open(filename,"r") 
			content1= input_content.readlines()
			content=[]
			for x in content1:
				x=x.split("  ")
				x[-1]=x[-1].strip()
				content.append(x)
				i=0
			#print(content)
			self.col_number.reverse()
			for index in col_number:
				content.sort(key=lambda content:content[index+i]) 
				for x in content:
					x.insert(0,x[index+i])
				i=i+1

			if self.sort_by=="asc":
				pass
			elif self.sort_by=="desc":
				content.reverse()
			
			f=open(filename, "w")
			print("writing to file:")
			for x in content:
				aa=""
				for x1 in x:
					aa=aa+x1+" "
				f.write(aa.rstrip()+"\n")
			f.close()
			
		finally:
			tl.release()		




start_time = time.time()
print("####### Execution Started #######")
	
input_file=sys.argv[1]
output_file=sys.argv[2]
mm_size=sys.argv[3]

col_names = []


f=open("metadata.txt","r")
cur=f.readline()
tuple_size=0
while(cur!=""):
	list1=cur.strip("\n").split(",")
	tuple_size += int(list1[1])
	cur=f.readline()
f.close()

tuplesinchunk=math.floor(int(sys.argv[3])*1000000/tuple_size)

threading=False
threadcount=0
if(sys.argv[4].lower()=="asc" or sys.argv[4].lower()=="desc"): 
	sort_by=sys.argv[4]
	for i in range(5,len(sys.argv)):
		col_names.append(sys.argv[i])	
else:
	threadcount=int(sys.argv[4])	
	sort_by=sys.argv[5]
	tuplesinchunk//=threadcount
	for i in range(6,len(sys.argv)):
		col_names.append(sys.argv[i])
		threading=True

if(tuplesinchunk==0 or len(sys.argv)<6):
	exit()

	
col_size=len(col_names)
col_number=[]
col_number = colname_to_colnum(col_names)

if len(col_number) == len(col_names):
	file = open(input_file,"r") 
	no_of_rows = 0
	  
	Content = file.read() 
	CoList = Content.split("\n")  
	for i in CoList: 
		if i:
			no_of_rows += 1
	
	no_of_file=createInitialRuns(input_file,tuplesinchunk,no_of_rows)

	print("------------------------------------------------------------------")
	print("####### Phase 1 started #######")
	print("Number of subfiles:",no_of_file)

	if(threading==False):	
		i=1
		#print(no_of_file)
		while i<=int(no_of_file):
		    sort_each_file(str(i)+".txt",sort_by,col_number)
		    i=i+1
		    #print("&&&&&&&&&&&&")
	else:
		global tl 
		tl=BoundedSemaphore(threadcount)
		tc=0
		threads=[]
		for i in range(1,int(no_of_file)+1):
			while(tc==threadcount):
				pass
			tc+=1			
			t=chunk_sort_threading(i,sort_by,col_number)
			threads.append(t)			
			tc-=1
		for i in threads:
			i.join()

	
	print()
	print("###### Phase 2 started ######")			
	print("------------------------Sorting-----------------------")
	print("-------------Writing to disk -------")
	for i in range(35):
		print(".",end="")
	print()
	
	mergeFiles(output_file,no_of_file,sort_by,col_size)

	print("####### Execution Completed #######")
	print("--- %s seconds ---" % (time.time() - start_time))
	
else:
	print("Columns doesn't match")