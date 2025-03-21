#include "StompListener.h"
#include <iostream>

StompListener::StompListener(StompProtocol* protocol, std::mutex& connectionMutex)
    : connectionMutex(connectionMutex), protocol(protocol), communicationThread() {}

// Destructor
StompListener::~StompListener() {
    stop();
}

void StompListener::run() {
    while (true) {
        ConnectionHandler* connectionHandler = protocol->getConnectionHandler();
        if (!connectionHandler) {
            protocol->waitForConnection();
            continue;
        }

        std::string frame;
        bool success = connectionHandler->getFrameAscii(frame, '\0');
        if (!success) {
            std::cerr << "[ERROR] Failed to read frame from socket for user: " << protocol->getUsername() << std::endl;
            protocol->clearConnectionHandler();
            continue;
        }

      
        try {
            protocol->parseFrame(frame);
        } catch (const std::exception& e) {
            std::cerr << "[ERROR] Exception while parsing frame: " << e.what() << std::endl;
        }
    }
}


// Start network listener thread
void StompListener::start() {
    communicationThread = std::thread(&StompListener::run, this);
}

// Stop network listener thread
void StompListener::stop() {
    if (communicationThread.joinable()) {
        communicationThread.join();
    }
}
