#ifndef STOMP_PROTOCOL_H
#define STOMP_PROTOCOL_H

#include <string>
#include <unordered_map>
#include <sstream>
#include "ConnectionHandler.h"
#include "event.h"



class StompProtocol {
public:
    // Constructor
    StompProtocol();

    void waitForConnection();
    void setConnectionHandler(const std::string& hostPort);
    void clearConnectionHandler(); // Set connection to null


    // Frame creation methods
    std::string generateConnectFrame(const std::string& username, const std::string& password);
    std::string generateSubscribeFrame(const std::string& channel, int subscriptionId, int receiptId);
    std::string generateUnsubscribeFrame(const std::string& channel);
    std::string generateSendFrame(const std::string& channel, const std::string& message);
    std::string generateDisconnectFrame(int receiptId);
    ConnectionHandler* getConnectionHandler();
    std::mutex& getConnectionMutex();
    // Frame parsing and dispatch
    void parseFrame(const std::string& frame);

    // Command processing
    std::string processCommand(const std::string& command);
    const std::string& getUsername() const;
    

private:
    // Frame handling methods
    void handleConnectedFrame(const std::unordered_map<std::string, std::string>& headers);
    void handleMessageFrame(const std::unordered_map<std::string, std::string>& headers, std::istringstream& bodyStream);
    void handleReceiptFrame(const std::unordered_map<std::string, std::string>& headers);
    void handleErrorFrame(const std::unordered_map<std::string, std::string>& headers, std::istringstream& bodyStream);
    
    void handleReportCommand(const std::string& filePath);
    void handleSummaryCommand(const std::string& channel, const std::string& user, const std::string& filePath);

    std::string epoch_to_date(int epochTime);
    static std::string trim(const std::string& str);
    

    void resetState();
    

    // Member variables
    std::unique_ptr<ConnectionHandler> connectionHandler;
    std::mutex connectionMutex;
    std::condition_variable connectionCV; 
    std::unordered_map<std::string, int> subscriptions;
    std::unordered_map<std::string, std::pair<std::string, std::string>> pendingReceipts;
    std::unordered_map<std::string, std::vector<Event>> savedEvents;
    std::string loggedName;
    int subscriptionIdCounter;
    int receiptIdCounter;
    bool isLoggedIn;
    
};

#endif // STOMP_PROTOCOL_H
