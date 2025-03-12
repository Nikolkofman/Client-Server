#ifndef USERINPUTHANDLER_H
#define USERINPUTHANDLER_H

#include "StompProtocol.h"
#include <mutex>
#include <thread>

class UserInputHandler {  
public:
    UserInputHandler(StompProtocol* protocol, std::mutex& connectionMutex);
    ~UserInputHandler();

    void start();
    void stop();
    void run();

    UserInputHandler(const UserInputHandler&) = delete;
    UserInputHandler& operator=(const UserInputHandler&) = delete;

private:
    std::mutex& connectionMutex;
    StompProtocol* protocol;
    std::thread inputThread;
};

#endif // USERINPUTHANDLER_H
