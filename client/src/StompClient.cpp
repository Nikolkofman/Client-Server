#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "../include/keyboardInput.h"
#include "../include/StompProtocol.h"
#include "../include/ConnectionHandler.h"
#include "../include/CommunicationHandler.h"

// Global synchronization objects
std::mutex mutex;


int main() {
    std::unique_ptr<StompProtocol> protocol(new StompProtocol());

    KeyboardHandler keyboardHandler(protocol.get(), protocol->getConnectionMutex());
    CommunicationHandler communicationHandler(protocol.get(), protocol->getConnectionMutex());

    keyboardHandler.start();
    communicationHandler.start();

    return 0;
}
