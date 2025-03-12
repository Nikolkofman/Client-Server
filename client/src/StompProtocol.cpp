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

StompProtocol::StompProtocol()
: connectionHandler(nullptr), connectionMutex(), connectionCV(), 
  subscriptions(), pendingReceipts(), savedEvents(), loggedName(""),
  subscriptionIdCounter(0), receiptIdCounter(0), isLoggedIn(false) {}


void StompProtocol::waitForConnection() {
    std::unique_lock<std::mutex> lock(connectionMutex);  // Lock to protect connectionHandler
    connectionCV.wait(lock, [this] { return connectionHandler != nullptr; });
}

void StompProtocol::setConnectionHandler(const std::string& hostPort) {
    // Parse the host and port from the string format "host:port"
    std::lock_guard<std::mutex> lock(connectionMutex);  // Lock to protect connectionHandler
    
    if(connectionHandler != nullptr){
        return;
    }
    size_t colonPos = hostPort.find(':');
    std::string host = hostPort.substr(0, colonPos);
    int port = std::stoi(hostPort.substr(colonPos + 1));
   
    // Initialize the connectionHandler object with the provided host and port
    connectionHandler = std::unique_ptr<ConnectionHandler>(new ConnectionHandler(host, port));
    if(connectionHandler->connect()){
        connectionCV.notify_one();
    }
    // Notify any waiting threads that the connection handler is now initialized
    //connectionCV.notify_all();
}

void StompProtocol::clearConnectionHandler(){
    connectionHandler = nullptr;
}

ConnectionHandler* StompProtocol::getConnectionHandler() {
        return connectionHandler.get();  // Return the connectionHandler pointer
    }


const std::string& StompProtocol::getUsername() const {
    return loggedName;
}



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

        int receiptId = receiptIdCounter++;
        int subscriptionId = subscriptionIdCounter++;
        pendingReceipts[std::to_string(receiptId)] = std::make_pair("join", channel);
        subscriptions[channel] = subscriptionId;

        std::string frame = generateSubscribeFrame(channel, subscriptionId, receiptId);
        connectionHandler->sendFrameAscii(frame, '\0');
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
            std::cerr << "Please login first." << std::endl;
            return "";
        }
    
  
        int disconnectReceiptId = receiptIdCounter++;
        pendingReceipts[std::to_string(disconnectReceiptId)] = std::make_pair("logout", "");
    
        std::string disconnectFrame = generateDisconnectFrame(disconnectReceiptId);
        connectionHandler->sendFrameAscii(disconnectFrame, '\0');
    
        return "";
    }
    
    

    std::cerr << "Unknown command: " << action << ". Type 'help' for usage.\n";
    return "";
}


std::string StompProtocol::generateSendFrame(const std::string& channel, const std::string& message) {
    std::ostringstream frame;
    frame << "SEND\n"
          << "destination:/" << channel << "\n"
          << "content-type:text/plain\n\n" 
          << message << "\n\0";
    return frame.str();
}




std::string StompProtocol::generateSubscribeFrame(const std::string& channel, int subscriptionId, int receiptId) {
    std::ostringstream frame;
    frame << "SUBSCRIBE\n"
          << "destination:/" << channel << "\n"
          << "id:" << subscriptionId << "\n"
          << "receipt:" << receiptId << "\n\n\0";

    return frame.str();
}


std::string StompProtocol::generateConnectFrame(const std::string& username, const std::string& password) {
    std::ostringstream frame;
    frame << "CONNECT\n"
          << "accept-version:1.2\n"
          << "host:stomp.cs.bgu.ac.il\n"
          << "login:" << username << "\n"
          << "passcode:" << password << "\n\n\0";
          
    return frame.str();
}

std::string StompProtocol::generateUnsubscribeFrame(const std::string& channel) {
    if (subscriptions.find(channel) == subscriptions.end()) {
        throw std::runtime_error("Channel not subscribed: " + channel);
    }

    int subscriptionId = subscriptions[channel];
    int receiptId = receiptIdCounter++;

    std::ostringstream frame;
    frame << "UNSUBSCRIBE\n"
          << "id:" << subscriptionId << "\n"
          << "receipt:" << receiptId << "\n\n\0";

    
    return frame.str();
}

std::string StompProtocol::generateDisconnectFrame(int receiptId) {
    std::ostringstream frame;
    frame << "DISCONNECT\n"
          << "accept-version:1.2\n"  
          << "receipt:" << receiptId << "\n\n\0";  // ✅ Fix null termination

    return frame.str();
}





void StompProtocol::parseFrame(const std::string& frame) {
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


void StompProtocol::handleConnectedFrame(const std::unordered_map<std::string, std::string>& headers) {
    auto versionIt = headers.find("version");
    if (versionIt != headers.end()) {
        std::cout << "Login successful" << std::endl;
        isLoggedIn = true;
    } else {
        throw std::runtime_error("Malformed CONNECTED frame: missing version header");
    }
}
void StompProtocol::handleMessageFrame(const std::unordered_map<std::string, std::string>& headers, std::istringstream& bodyStream) {
    auto destinationIt = headers.find("destination");
    if (destinationIt != headers.end()) {
        std::string channel = destinationIt->second;

        if (!channel.empty() && channel[0] == '/') {
            channel = channel.substr(1); 
        }

        std::string body;
        std::getline(bodyStream, body, '\0');

        Event event(body);
        event.setChannelName(channel);  

        savedEvents[channel].push_back(event);  
    } else {
        std::cerr << "Malformed MESSAGE frame: missing destination header" << std::endl;
    }
}



void StompProtocol::handleReceiptFrame(const std::unordered_map<std::string, std::string>& headers) {
    auto receiptIdIt = headers.find("receipt-id");
    if (receiptIdIt == headers.end()) {
        std::cerr << "Warning: RECEIPT frame received but no receipt-id found!" << std::endl;
        return;
    }

    std::string receiptID = receiptIdIt->second;
    
    if (pendingReceipts.find(receiptID) == pendingReceipts.end()) {
        std::cerr << "Warning: RECEIPT ID " << receiptID << " not found in pendingReceipts!" << std::endl;
        
        // Debugging: Print all stored receipt IDs
        std::cerr << "Stored receipt IDs: ";
        for (const auto& entry : pendingReceipts) {
            std::cerr << entry.first << " ";
        }
        std::cerr << std::endl;
        
        return;
    }
    
    auto actionIt = pendingReceipts.find(receiptID);
    std::string action = actionIt->second.first;
    std::string channel = actionIt->second.second;
    
    if (action == "join") {
        std::cout << "Joined channel " << channel << std::endl;
    } else if (action == "exit") {
        subscriptions.erase(channel);
        std::cout << "Exited channel " << channel << std::endl;
    } else if (action == "exit_logout") {
        subscriptions.erase(channel);
        std::cout << "Exited channel " << channel << " (preparing for logout)" << std::endl;

        // ✅ If all channels are unsubscribed, proceed with logout
        if (subscriptions.empty()) {
            std::cout << "All channels exited. Proceeding with logout..." << std::endl;
            int disconnectReceiptId = receiptIdCounter++;
            pendingReceipts[std::to_string(disconnectReceiptId)] = std::make_pair("logout", "");
            std::string disconnectFrame = generateDisconnectFrame(disconnectReceiptId);
            connectionHandler->sendFrameAscii(disconnectFrame, '\0');
        }
    } else if (action == "logout") {
        std::cout << "Logged out" << std::endl; 
        resetState();
    }
    
    pendingReceipts.erase(receiptID);
}


void StompProtocol::handleErrorFrame(const std::unordered_map<std::string, std::string>& headers, std::istringstream& bodyStream) {
    auto messageIt = headers.find("message");
    std::string errorMessage = (messageIt != headers.end()) ? messageIt->second : "Unknown error";

    std::string body;
    std::getline(bodyStream, body, '\0');

    // ✅ Better error handling for specific cases
    if (errorMessage.find("Password does not match") != std::string::npos) {
        std::cout << "Wrong password" << std::endl;
    } else {
        std::cerr << "Error: " << errorMessage << std::endl;
        if (!body.empty()) {
            std::cerr << "Details: " << body << std::endl;
        }
    }

    // ✅ Check for receipt ID and remove from pending receipts
    auto receiptIdIt = headers.find("receipt-id");
    if (receiptIdIt != headers.end()) {
        std::string receiptId = receiptIdIt->second;
        pendingReceipts.erase(receiptId);
    }

    // ✅ Only disconnect if it's a severe error
    if (errorMessage.find("Server shutting down") != std::string::npos ||
        errorMessage.find("Unauthorized") != std::string::npos) {
        
        if (connectionHandler) {
            int disconnectReceiptId = receiptIdCounter++;
            pendingReceipts[std::to_string(disconnectReceiptId)] = std::make_pair("logout", "");
            
            std::string frame = generateDisconnectFrame(disconnectReceiptId);
            connectionHandler->sendFrameAscii(frame, '\0');

            resetState();
        }
    }
}

void StompProtocol::handleReportCommand(const std::string& filePath) {
    names_and_events parsedData = parseEventsFile(filePath);
    std::string channel_name = trim(parsedData.channel_name);

    if (subscriptions.find(channel_name) == subscriptions.end()) {
        std::cerr << "Error: You are not subscribed to channel [" << channel_name << "]." << std::endl;
        return;
    }

    if (!connectionHandler) {
        std::cerr << "Error: Connection to server lost. Cannot send event reports." << std::endl;
        return;
    }

    const std::vector<Event>& events = parsedData.events;
    for (Event event : events) {  
        event.setEventOwnerUser(trim(loggedName));
        event.setChannelName(channel_name);  
        savedEvents[channel_name].push_back(event);

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
        connectionHandler->sendFrameAscii(frame, '\0');
    }

    std::cout << "Reported" << std::endl;  
}



void StompProtocol::handleSummaryCommand(const std::string& channel, const std::string& user, const std::string& filePath) {
    std::ofstream outFile(filePath, std::ios::trunc);
    if (!outFile.is_open()) {
        std::cerr << "Error: Could not create file " << filePath << std::endl;
        return;
    }

    std::string formattedChannel = channel;

    if (savedEvents.find(formattedChannel) == savedEvents.end()) {
        if (savedEvents.find("/" + channel) != savedEvents.end()) {
            formattedChannel = "/" + channel;
        } else if (savedEvents.find("//" + channel) != savedEvents.end()) {
            formattedChannel = "//" + channel;
        }
    }

    auto channelIt = savedEvents.find(formattedChannel);
    if (channelIt == savedEvents.end()) {
        outFile << "No events found for channel: " << formattedChannel << "\n";
        outFile.close();
        return;
    }

    std::vector<Event> filteredEvents;
    for (const auto& event : channelIt->second) {
        std::string trimmedOwner = trim(event.getEventOwnerUser());
        if (trimmedOwner == trim(user)) {
            filteredEvents.push_back(event);
        }
    }

    if (filteredEvents.empty()) {
        outFile << "No events found from user: " << user << " on channel: " << formattedChannel << "\n";
        outFile.close();
        return;
    }

    std::sort(filteredEvents.begin(), filteredEvents.end(), [](const Event& a, const Event& b) {
        return (a.get_date_time() < b.get_date_time()) || 
               (a.get_date_time() == b.get_date_time() && a.get_name() < b.get_name());
    });

    outFile << "Channel " << formattedChannel << "\n"
            << "Stats:\n"
            << "Total: " << filteredEvents.size() << "\n"
            << "active: " << std::count_if(filteredEvents.begin(), filteredEvents.end(), 
                           [](const Event& e){ return e.get_general_information().at("active") == "true"; }) << "\n"
            << "forces arrival at scene: " << std::count_if(filteredEvents.begin(), filteredEvents.end(), 
                           [](const Event& e){ return e.get_general_information().at("forces_arrival_at_scene") == "true"; }) << "\n\n";

    outFile << "Event Reports:\n";
    for (size_t i = 0; i < filteredEvents.size(); i++) {
        const Event& event = filteredEvents[i];

        outFile << "Report_" << (i + 1) << ":\n"
                << "\tcity: " << event.get_city() << "\n"
                << "\tdate time: " << epoch_to_date(event.get_date_time()) << "\n"
                << "\tevent name: " << event.get_name() << "\n"
                << "\tsummary: " 
                << (event.get_description().size() > 27 ? event.get_description().substr(0, 27) + "..." : event.get_description()) 
                << "\n\n";
    }

    outFile.close();
    std::cout << "Summary successfully written to " << filePath << std::endl;  
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
               << timeInfo.tm_year + 1900 << " "  
               << std::setw(2) << timeInfo.tm_hour << ":"
               << std::setw(2) << timeInfo.tm_min;

    return dateStream.str();
}


void StompProtocol::resetState() {
    isLoggedIn = false;
    subscriptions.clear();
    pendingReceipts.clear();
    subscriptionIdCounter = 1;
    receiptIdCounter = 1;

   
    if (connectionHandler) {
        try {
            connectionHandler->close(); 
        } catch (const std::exception& e) {
            std::cerr << "Error closing connection: " << e.what() << std::endl;
        }
        connectionHandler.reset(); 
    }
}


std::mutex& StompProtocol::getConnectionMutex() {
    return connectionMutex;
}