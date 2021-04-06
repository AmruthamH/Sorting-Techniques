  
all:
	g++ -std=c++11 -g -pthread mergesort.cpp main.cpp minheap.cpp -o ms.out