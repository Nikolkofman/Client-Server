CFLAGS:=-c -Wall -Weffc++ -g -std=c++17 -Iinclude
LDFLAGS:=-lboost_system -lpthread
# TODO change the makefile for StompEMIlient
all: clean StompEMIClient

EchoClient: bin/ConnectionHandler.o bin/echoClient.o
	g++ -o bin/EchoClient bin/ConnectionHandler.o bin/echoClient.o $(LDFLAGS)

StompEMIClient: bin/ConnectionHandler.o bin/StompClient.o bin/event.o bin/UserInputHandler.o bin/StompListener.o bin/StompProtocol.o 
	g++ -o bin/StompEMIClient bin/ConnectionHandler.o bin/StompClient.o bin/event.o bin/UserInputHandler.o bin/StompListener.o bin/StompProtocol.o  $(LDFLAGS)

bin/ConnectionHandler.o: src/ConnectionHandler.cpp
	g++ $(CFLAGS) -o bin/ConnectionHandler.o src/ConnectionHandler.cpp

bin/echoClient.o: src/echoClient.cpp
	g++ $(CFLAGS) -o bin/echoClient.o src/echoClient.cpp

bin/event.o: src/event.cpp
	g++ $(CFLAGS) -o bin/event.o src/event.cpp

bin/StompClient.o: src/StompClient.cpp
	g++ $(CFLAGS) -o bin/StompClient.o src/StompClient.cpp

bin/UserInputHandler.o: src/UserInputHandler.cpp
	g++ $(CFLAGS) -o bin/UserInputHandler.o src/UserInputHandler.cpp

bin/StompListener.o: src/StompListener.cpp
	g++ $(CFLAGS) -o bin/StompListener.o src/StompListener.cpp

bin/StompProtocol.o: src/StompProtocol.cpp
	g++ $(CFLAGS) -o bin/StompProtocol.o src/StompProtocol.cpp



.PHONY: clean
clean:
	rm -f bin/*
