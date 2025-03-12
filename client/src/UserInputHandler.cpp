#include "../include/UserInputHandler.h"  
#include <iostream>
#include <sstream>

UserInputHandler::UserInputHandler(StompProtocol* protocol, std::mutex& connectionMutex)
    : connectionMutex(connectionMutex), protocol(protocol), inputThread() {}


void UserInputHandler::run() {
    std::string command;
    while (true) {
        std::getline(std::cin, command); 
        if (command.empty()) continue;

       
        std::string frame = protocol->processCommand(command);
        
        if (!frame.empty()) {
            ConnectionHandler* connectionHandler = protocol->getConnectionHandler();
            if (connectionHandler != nullptr) {
                std::lock_guard<std::mutex> lock(connectionMutex);  
                connectionHandler->sendFrameAscii(frame, '\0');
            }
        }
    }
}

void UserInputHandler::start() {
    inputThread = std::thread(&UserInputHandler::run, this);
}

void UserInputHandler::stop() {
    if (inputThread.joinable()) {
        inputThread.join();
    }
}
 
UserInputHandler::~UserInputHandler() {
    stop();
}
