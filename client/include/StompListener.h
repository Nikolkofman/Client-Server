
#include <mutex>
#include <thread>
#include "StompProtocol.h"

class StompListener {
    private:
        std::mutex& connectionMutex;
        StompProtocol* protocol;
        std::thread communicationThread;
    
    public:
    StompListener(StompProtocol* protocol, std::mutex& connectionMutex);
     
        StompListener(const StompListener&) = delete;
        StompListener& operator=(const StompListener&) = delete;
        StompListener(StompListener&&) noexcept = default;
        StompListener& operator=(StompListener&&) noexcept = default;
            
            ~StompListener();
            
            void run();
            void start();
            void stop();
        };


        
