#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "../include/UserInputHandler.h"
#include "../include/StompProtocol.h"
#include "../include/ConnectionHandler.h"
#include "../include/StompListener.h"

// Global synchronization objects
std::mutex mutex;


int main() {
   std::shared_ptr<StompProtocol> protocol = std::make_shared<StompProtocol>();



    UserInputHandler UserInputHandler(protocol.get(), protocol->getConnectionMutex());
    StompListener StompListener(protocol.get(), protocol->getConnectionMutex());

    UserInputHandler.start();
    StompListener.start();

    return 0;
}
