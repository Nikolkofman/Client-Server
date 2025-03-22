#include "StompProtocol.h"
#include "ConnectionHandler.h"
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include "event.h"
#include <iomanip>
#include <algorithm>
#include <filesystem>
#include <atomic>


StompProtocol::StompProtocol()
    : connectionHandler(nullptr),
      connectionMutex(), connectionCV(),
      logoutMutex(), eventsMutex(), subscriptionMutex(),
      subscriptions(), userSubscriptions(), pendingReceipts(), savedEvents(),
      loggedName(""), isLoggedIn(false) {}

void StompProtocol::waitForConnection() {
    std::unique_lock<std::mutex> lock(connectionMutex);  // Lock to protect connectionHandler
    connectionCV.wait(lock, [this] { return connectionHandler != nullptr; });
}

void StompProtocol::setConnectionHandler(const std::string& hostPort) {
    std::lock_guard<std::mutex> lock(connectionMutex);

    if(connectionHandler != nullptr) {
        std::cerr << "[ERROR] ConnectionHandler already exists for user: " << loggedName << std::endl;
        return;
    }

    size_t colonPos = hostPort.find(':');
    std::string host = hostPort.substr(0, colonPos);
    int port = std::stoi(hostPort.substr(colonPos + 1));

    connectionHandler = std::unique_ptr<ConnectionHandler>(new ConnectionHandler(host, port));

    if(connectionHandler->connect()) {
        connectionCV.notify_one();
    } else {
        std::cerr << "[ERROR] Connection failed for user: " << loggedName << std::endl;
    }
}

void StompProtocol::clearConnectionHandler() {
    std::lock_guard<std::mutex> lock(connectionMutex);
    if (connectionHandler) {
        connectionHandler->close();
        connectionHandler.reset();
    }
}



ConnectionHandler* StompProtocol::getConnectionHandler() {
        return connectionHandler.get(); 
    }


const std::string& StompProtocol::getUsername() const {
    return loggedName;
}


// processCommand method
std::string StompProtocol::processCommand(const std::string& command) {
    std::istringstream stream(command);
    std::string action;
    stream >> action;

    if (action == "login") {
        if (isLoggedIn) {
            std::cerr << "Already logged in." << std::endl;
            return "";
        }
        std::string port, username, password;
        if (!(stream >> port >> username >> password)) {
            std::cerr << "Usage: login <host:port> <username> <password>\n";
            return "";
        }
        loggedName = username;
        setConnectionHandler(port);
        return generateConnectFrame(username, password);
    } 
    else if (action == "join") {
        if (!isLoggedIn) {
            std::cerr << "Please login first." << std::endl;
            return "";
        }
    
        std::string channel;
        if (!(stream >> channel)) {
            std::cerr << "join command needs 1 argument: (channel_name)\n";
            return "";
        }
    
        if (userSubscriptions[loggedName].find(channel) != userSubscriptions[loggedName].end()) {
            std::cerr << "Already subscribed to " << channel << " as " << loggedName << std::endl;
            return "";
        }
    
        int receiptId = getNextReceiptId();
        int subscriptionId = getNextSubscriptionId();
    
        userSubscriptions[loggedName][channel] = subscriptionId;
        pendingReceipts[std::to_string(receiptId)] = std::make_pair("join", channel);
    
        std::string frame = generateSubscribeFrame(channel, subscriptionId, receiptId);
        if (!connectionHandler->sendFrameAscii(frame, '\0')) {
            std::cerr << "[ERROR] Failed to send SUBSCRIBE frame for " << channel << std::endl;
            return "";
        }
    
        return "";
    }
    
    else if (action == "report") {
        if (!isLoggedIn) {
            std::cerr << "Please login first." << std::endl;
            return "";
        }

        std::string filePath;
        if (!(stream >> filePath)) {
            std::cerr << "report command needs 1 argument: (file)\n";
            return "";
        }

        handleReportCommand(filePath);
        return "";
    }
    else if (action == "summary") {
        if (!isLoggedIn) {
            std::cerr << "Please login first." << std::endl;
            return "";
        }

        std::string channel, user, filePath;
        if (!(stream >> channel >> user >> filePath)) {
            std::cerr << "summary command needs 3 arguments: (channel) (user) (file)\n";
            return "";
        }

        handleSummaryCommand(channel, user, filePath);
        return "";
    }
    else if (action == "logout") {
        if (!isLoggedIn) {
            std::cerr << "[ERROR] Cannot logout: User is not logged in." << std::endl;
            return "";
        }
    
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
        if (connectionHandler) {
            std::cerr << "logged out" << std::endl;
            connectionHandler->close();  
            connectionHandler.reset();  
        }
    
        resetState();
        return "";
    }
    
    std::cerr << "Unknown command: " << action << ". Type 'help' for usage.\n";
    return "";
}

// Send frame 
std::string StompProtocol::generateSendFrame(const std::string& channel, const std::string& message) {
    std::ostringstream frame;
    frame << "SEND\n"
          << "destination:/" << channel << "\n"
          << "user:" << loggedName << "\n"  
          << "content-type:text/plain\n\n" 
          << message << "\n\0";
    return frame.str();
}


// Subscribe frame
std::string StompProtocol::generateSubscribeFrame(const std::string& channel, int subscriptionId, int receiptId) {
    std::ostringstream frame;
    frame << "SUBSCRIBE\n"
          << "destination:/" << channel << "\n"
          << "id:" << subscriptionId << "\n"
          << "receipt:" << receiptId << "\n\n\0";

    return frame.str();
}

// Connect frame
std::string StompProtocol::generateConnectFrame(const std::string& username, const std::string& password) {
    std::ostringstream frame;
    frame << "CONNECT\n"
          << "accept-version:1.2\n"
          << "host:stomp.cs.bgu.ac.il\n"
          << "login:" << username << "\n"
          << "passcode:" << password << "\n\n\0";
          
    return frame.str();
}
// Unsubscribe frame
std::string StompProtocol::generateUnsubscribeFrame(const std::string& channel, int subscriptionId, int receiptId) {
    std::ostringstream frame;
    frame << "UNSUBSCRIBE\n"
          << "id:" << subscriptionId << "\n"
          << "receipt:" << receiptId << "\n\n\0";

    return frame.str();
}

// Disconnect frame
std::string StompProtocol::generateDisconnectFrame(int receiptId) {
    std::ostringstream frame;
    frame << "DISCONNECT\n"
          << "receipt:" << receiptId << "\n\n\0"; 

    return frame.str();
}


// Parse frame
void StompProtocol::parseFrame(const std::string& frame) {
    std::lock_guard<std::mutex> lock(connectionMutex);  

    std::istringstream stream(frame);
    std::string command;
    std::getline(stream, command);

    std::unordered_map<std::string, std::string> headers;
    std::string line;

    while (std::getline(stream, line) && !line.empty()) {
        size_t colonPos = line.find(':');
        if (colonPos != std::string::npos) {
            std::string key = line.substr(0, colonPos);
            std::string value = line.substr(colonPos + 1);
            headers[key] = value;
        }
    }

    if (command == "CONNECTED") {
        handleConnectedFrame(headers);
    } else if (command == "MESSAGE") {
        handleMessageFrame(headers, stream);
    } else if (command == "RECEIPT") {
        handleReceiptFrame(headers);
    } else if (command == "ERROR") {
        handleErrorFrame(headers, stream);
    }
}

// Handle connected frame
void StompProtocol::handleConnectedFrame(const std::unordered_map<std::string, std::string>& headers) {
    auto versionIt = headers.find("version");
    if (versionIt != headers.end()) {
        std::cout << "Login successful" << std::endl;
        isLoggedIn = true;
    } else {
        throw std::runtime_error("Malformed CONNECTED frame: missing version header");
    }
}

// Handle message frame
void StompProtocol::handleMessageFrame(const std::unordered_map<std::string, std::string>& headers, std::istringstream& bodyStream) {
    auto destinationIt = headers.find("destination");
    if (destinationIt == headers.end()) {
        std::cerr << "[ERROR] Malformed MESSAGE frame: missing destination header" << std::endl;
        return;
    }

    std::string channel = destinationIt->second;
    if (!channel.empty() && channel[0] == '/') {
        channel = channel.substr(1);
    }

    std::string eventOwner = loggedName;
    auto userIt = headers.find("user");
    if (userIt != headers.end()) {
        eventOwner = trim(userIt->second);
    }

    std::string body;
    std::getline(bodyStream, body, '\0');

    Event event(body);
    event.setChannelName(channel);
    event.setEventOwnerUser(eventOwner);

    
    {
        std::lock_guard<std::mutex> lock(eventsMutex);
        savedEvents[channel].push_back(event);
    }
}

// Handle receipt frame
void StompProtocol::handleReceiptFrame(const std::unordered_map<std::string, std::string>& headers) {
    auto receiptIdIt = headers.find("receipt-id");

    if (receiptIdIt == headers.end()) {
        std::cerr << "[ERROR] RECEIPT frame received without receipt-id!" << std::endl;
        return;
    }

    std::string receiptID = trim(receiptIdIt->second);
    if (pendingReceipts.find(receiptID) == pendingReceipts.end()) {
        std::cerr << "[WARNING] RECEIPT ID '" << receiptID << "' not found in pendingReceipts!" << std::endl;
        return;
    }

    auto actionIt = pendingReceipts.find(receiptID);
    std::string action = actionIt->second.first;

    if (action == "join") {
        std::cout << "Joined channel " << actionIt->second.second << std::endl;
    } else if (action == "logout") {
        std::cout << "Logged out" << std::endl;
        resetState();
    } else if (action == "summary" || action == "report") {
        std::cerr << "[DEBUG] Clearing stuck receipt for action: " << action << std::endl;
    }

    pendingReceipts.erase(receiptID);
}


// Handle error frame
void StompProtocol::handleErrorFrame(const std::unordered_map<std::string, std::string>& headers, std::istringstream& bodyStream) {
    auto messageIt = headers.find("message");
    std::string errorMessage = (messageIt != headers.end()) ? messageIt->second : "Unknown error";

    std::string body;
    std::getline(bodyStream, body, '\0');

    if (errorMessage.find("Password does not match") != std::string::npos) {
        std::cout << "Wrong password" << std::endl;
    } else {
        std::cerr << "Error: " << errorMessage << std::endl;
        if (!body.empty()) {
            std::cerr << "Details: " << body << std::endl;
        }
    }

    auto receiptIdIt = headers.find("receipt-id");
    if (receiptIdIt != headers.end()) {
        std::string receiptId = receiptIdIt->second;
        pendingReceipts.erase(receiptId);
    }

    if (errorMessage.find("Server shutting down") != std::string::npos ||
        errorMessage.find("Unauthorized") != std::string::npos) {
        
        if (connectionHandler) {
            int disconnectReceiptId = globalReceiptCounter.fetch_add(1);  

            pendingReceipts[std::to_string(disconnectReceiptId)] = std::make_pair("logout", "");
            
            std::string frame = generateDisconnectFrame(disconnectReceiptId);
            connectionHandler->sendFrameAscii(frame, '\0');

            resetState();
        }
    }
}

// Report command
void StompProtocol::handleReportCommand(const std::string& filePath) {
    if (!isLoggedIn) {
        std::cerr << "Error: Cannot report, user not logged in." << std::endl;
        return;
    }

    names_and_events parsedData = parseEventsFile(filePath);
    std::string channel_name = trim(parsedData.channel_name);

    if (userSubscriptions[loggedName].find(channel_name) == userSubscriptions[loggedName].end()) {
        std::cerr << "Error: You are not subscribed to channel [" << channel_name << "]." << std::endl;
        return;
    }

    if (!connectionHandler || !connectionHandler->isOpen()) {
        std::cerr << "Error: Connection to server lost. Cannot send event reports." << std::endl;
        return;
    }

    for (Event event : parsedData.events) {  
        event.setEventOwnerUser(trim(loggedName));
        event.setChannelName(channel_name);  

        std::ostringstream body;
        body << "user: " << loggedName << "\n"
             << "city: " << event.get_city() << "\n"
             << "event name: " << event.get_name() << "\n"
             << "date time: " << event.get_date_time() << "\n"
             << "general information:\n";

        for (const auto& [key, value] : event.get_general_information()) {
            body << "\t" << key << ": " << value << "\n";
        }
        body << "description:\n" << event.get_description() << "\n";

        std::string frame = generateSendFrame("/" + channel_name, body.str());
        if (!connectionHandler->sendFrameAscii(frame, '\0')) {
            std::cerr << "Failed to send event report." << std::endl;
        }
    }

    std::cout << "Reported" << std::endl;
}

// Summary command
void StompProtocol::handleSummaryCommand(const std::string& channel, const std::string& user, const std::string& filePath) {
    std::lock_guard<std::mutex> lock(eventsMutex);

    std::ofstream outFile(filePath, std::ios::trunc);
    if (!outFile.is_open()) {
        std::cerr << "[ERROR] Could not create or open file: " << filePath << std::endl;
        return;
    }

    
    std::string formattedChannel = trim(channel);
    if (!formattedChannel.empty() && formattedChannel[0] == '/') {
        formattedChannel = formattedChannel.substr(1);
    }

    auto channelIt = savedEvents.find(formattedChannel);
    if (channelIt == savedEvents.end()) {
        std::string alternativeChannel = "/" + formattedChannel;
        channelIt = savedEvents.find(alternativeChannel);

        if (channelIt == savedEvents.end()) {
            std::cerr << "[ERROR] No events found for channel: " << formattedChannel 
                      << " or alternative: " << alternativeChannel << std::endl;
            outFile << "No events found for channel: " << formattedChannel << "\n";
            outFile.close();
            return;
        }
    }

    std::vector<Event> filteredEvents;
    for (const auto& event : channelIt->second) {
        if (strcasecmp(event.getEventOwnerUser().c_str(), user.c_str()) == 0 || user == "all") {  
            filteredEvents.push_back(event);
        }
    }


    if (filteredEvents.empty()) {
        std::cerr << "[WARNING] No matching events found for user: " << user << std::endl;
        outFile << "No events found from user: " << user << " on channel: " << formattedChannel << "\n";
        outFile.close();
        return;
    }

    std::sort(filteredEvents.begin(), filteredEvents.end(), [](const Event& a, const Event& b) {
        return (a.get_date_time() < b.get_date_time()) || 
               (a.get_date_time() == b.get_date_time() && a.get_name() < b.get_name());
    });

    
auto isTrue = [](std::string value) {
    value = trim(value); 
    return value == "true" || value == "True" || value == "1"; 
};

    int activeCount = 0, forcesArrivalCount = 0;
    for (const auto& event : channelIt->second) {  
        const auto& info = event.get_general_information();

        if (info.count("active")) {
            std::string activeStatus = info.at("active");
        } 
        
        if (info.count("forces_arrival_at_scene")) {
            std::string forcesArrival = info.at("forces_arrival_at_scene");
        }
        
        if (info.count("active") && isTrue(info.at("active"))) {
            activeCount++;
        }
        if (info.count("forces_arrival_at_scene") && isTrue(info.at("forces_arrival_at_scene"))) {
            forcesArrivalCount++;
        }
    }


    outFile << "Channel: " << formattedChannel << "\n"
            << "Stats:\n"
            << "Total events: " << filteredEvents.size() << "\n"
            << "Active cases: " << activeCount << "\n"
            << "Forces arrived at scene: " << forcesArrivalCount << "\n\n";

    outFile << "Event Reports:\n\n";
    for (const Event& event : filteredEvents) {
        outFile << epoch_to_date(event.get_date_time()) << ":00 - " 
                << event.get_name() << " - " << event.get_city() << ":\n"
                << event.get_description() << "\n\n";
    }

    outFile.close();
    std::cerr << "[INFO] Summary successfully written to " << filePath << std::endl;
}


std::string StompProtocol::trim(const std::string& str) {  
    size_t start = str.find_first_not_of(" \t\n\r\f\v");
    size_t end = str.find_last_not_of(" \t\n\r\f\v");
    return (start == std::string::npos) ? "" : str.substr(start, end - start + 1);
}


std::string StompProtocol::epoch_to_date(int epochTime) {
    std::time_t time = static_cast<std::time_t>(epochTime);
    std::tm timeInfo;
    localtime_r(&time, &timeInfo);  

    std::ostringstream dateStream;
    dateStream << std::setfill('0') << std::setw(2) << timeInfo.tm_mday << "/"
               << std::setw(2) << (timeInfo.tm_mon + 1) << "/"
               << (timeInfo.tm_year + 1900) << " "
               << std::setw(2) << timeInfo.tm_hour << ":"
               << std::setw(2) << timeInfo.tm_min
               << ":00";

    return dateStream.str();
}

void StompProtocol::resetState() {
    std::lock_guard<std::mutex> lock(logoutMutex);
    isLoggedIn = false;
    loggedName.clear();
    pendingReceipts.clear();

    {
        std::lock_guard<std::mutex> lock(subscriptionMutex);
        userSubscriptions.erase(loggedName);
    }


    {
        std::lock_guard<std::mutex> lock(eventsMutex);
        for (auto it = savedEvents.begin(); it != savedEvents.end();) {
            it->second.erase(
                std::remove_if(it->second.begin(), it->second.end(),
                    [this](const Event& event) { return event.getEventOwnerUser() == loggedName; }),
                it->second.end()
            );
            if (it->second.empty()) {
                it = savedEvents.erase(it);
            } else {
                ++it;
            }
        }
    }

    if (connectionHandler) {
        try {
            connectionHandler->close();
        } catch (const std::exception& e) {
            std::cerr << "[ERROR] Closing connection: " << e.what() << std::endl;
        }
        connectionHandler.reset();
    }
}


std::mutex& StompProtocol::getConnectionMutex() {
    return connectionMutex;
}

std::atomic<int> StompProtocol::globalReceiptCounter(1);
std::mutex StompProtocol::receiptMutex;
int StompProtocol::getNextReceiptId() {
    std::lock_guard<std::mutex> lock(receiptMutex);  

    std::ifstream inFile("receipt_counter.txt");
    int id = 1; 

    if (inFile) {
        inFile >> id;  
        inFile.close();
    }

    std::ofstream outFile("receipt_counter.txt");
    outFile << (id + 1);
    outFile.close();

    return id;
}



std::mutex subscriptionMutex;

int StompProtocol::getNextSubscriptionId() {
    std::lock_guard<std::mutex> lock(subscriptionMutex);

    std::ifstream inFile("subscription_counter.txt");
    int id = 1;  

    if (inFile) {
        inFile >> id;
        inFile.close();
    }

    std::ofstream outFile("subscription_counter.txt");
    outFile << (id + 1);
    outFile.close();

    return id;
}
