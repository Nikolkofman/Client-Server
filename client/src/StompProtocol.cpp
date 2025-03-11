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
    std::cout<<"port: "<<port << " host:"<< host  << " hl "<< host.length()<<  std::endl;
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
        stream >> port >> username >> password;
        loggedName = username;
        setConnectionHandler(port);
        return createConnectFrame(username, password);
    } 
    else if (action == "join") {
        if (!isLoggedIn) {
            std::cerr << "Please login first." << std::endl;
            return "";
        }
        
        std::string channel;
        stream >> channel;
    
        int receiptId = receiptIdCounter++;  // Generate a unique receipt ID
        int subscriptionId = subscriptionIdCounter++;  // Generate a unique subscription ID
    
        // Store the subscription info before sending
        pendingReceipts[std::to_string(receiptId)] = std::make_pair("join", channel);
        subscriptions[channel] = subscriptionId;
    
        // FIX: Call `createSubscribeFrame(channel, subscriptionId, receiptId)`
        std::string frame = createSubscribeFrame(channel, subscriptionId, receiptId);
    
        connectionHandler->sendFrameAscii(frame, '\0');  // Send the frame
    
        return "";
    }
    
    
    else if (action == "exit") {
        if (!isLoggedIn) {
            std::cerr << "Please login first." << std::endl;
            return "";
        }
        std::string channel;
        stream >> channel;
        int receiptId = receiptIdCounter++;
        pendingReceipts[std::to_string(receiptId)] = std::make_pair("unsubscribe", channel);
        return createUnsubscribeFrame(channel);
    }  
    else if (action == "report") {
        if (!isLoggedIn) {
            std::cerr << "Please login first." << std::endl;
            return "";
        }
        std::string filePath;
        stream >> filePath;
        handleReportCommand(filePath);
        return "";
    } 
    else if (action == "summary") {
        if (!isLoggedIn) {
            std::cerr << "Please login first." << std::endl;
            return "";
        }
        std::string channel, user, filePath;
        stream >> channel >> user >> filePath;
        handleSummaryCommand(channel, user, filePath);
        return "";
    } 
    else if (action == "logout") {
        if (!isLoggedIn) {
            std::cerr << "Please login first." << std::endl;
            return "";
        }
        
        // Unsubscribe from all channels before logging out
        for (const auto& subscription : subscriptions) {
            int receiptId = receiptIdCounter++;
            pendingReceipts[std::to_string(receiptId)] = std::make_pair("exit_logout", subscription.first);
            std::string frame = createUnsubscribeFrame(subscription.first);
            connectionHandler->sendFrameAscii(frame, '\0');
        }
    
        // Create the disconnect frame
        std::string disconnectFrame = createDisconnectFrame(); 
    
        //std::cout << "Sending DISCONNECT frame:\n" << disconnectFrame << std::endl; 
    
        // Send the frame
        connectionHandler->sendFrameAscii(disconnectFrame, '\0');
    
        return "";
    }
    
    return "";
}

std::string StompProtocol::createSendFrame(const std::string& channel, const std::string& message) {
    std::ostringstream frame;
    frame << "SEND\n"
          << "destination:/" << channel << "\n"
          << "content-type:text/plain\n\n"  // ✅ Ensures proper message format
          << message << "\n\0";
    return frame.str();
}




std::string StompProtocol::createSubscribeFrame(const std::string& channel, int subscriptionId, int receiptId) {
    std::ostringstream frame;
    frame << "SUBSCRIBE\n"
          << "destination:/" << channel << "\n"
          << "id:" << subscriptionId << "\n"
          << "receipt:" << receiptId << "\n\n\0";

    return frame.str();
}


std::string StompProtocol::createConnectFrame(const std::string& username, const std::string& password) {
    std::ostringstream frame;
    frame << "CONNECT\n"
          << "accept-version:1.2\n"
          << "host:stomp.cs.bgu.ac.il\n"
          << "login:" << username << "\n"
          << "passcode:" << password << "\n\n\0";
          
    return frame.str();
}

std::string StompProtocol::createUnsubscribeFrame(const std::string& channel) {
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

std::string StompProtocol::createDisconnectFrame() {
    int receiptId = receiptIdCounter++;

    std::ostringstream frame;
    frame << "DISCONNECT\n"
          << "accept-version:1.2\n"  
          << "receipt:" << receiptId << "\n\0";

    pendingReceipts[std::to_string(receiptId)] = std::make_pair("logout", "");

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


        std::string body;
        std::getline(bodyStream, body, '\0');

        Event event(body);

       
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
    //std::cout << "Received RECEIPT ID: " << receiptID << std::endl;
    
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

    // Print only "Wrong password" if the error is password-related
    if (errorMessage.find("Password does not match") != std::string::npos) {
        std::cout << "Wrong password" << std::endl;
    } else {
        std::cerr << errorMessage << std::endl;
        if (!body.empty()) {
            std::cerr << body << std::endl;
        }
    }

    // Cleanup pending receipts if necessary
    auto receiptIdIt = headers.find("receipt-id");
    if (receiptIdIt != headers.end() && pendingReceipts.find(receiptIdIt->second) != pendingReceipts.end()) {
        pendingReceipts.erase(receiptIdIt->second);
    }

    // Disconnect only if it's a critical error
    std::string frame = createDisconnectFrame();
    if (connectionHandler->sendFrameAscii(frame, '\0')) {
        resetState();
    }
}

void StompProtocol::handleReportCommand(const std::string& filePath) {
    // Parse the JSON file
    names_and_events parsedData = parseEventsFile(filePath);
    std::string channel_name = parsedData.channel_name;

    // // 🔹 Ensure the correct channel format (avoid double slashes)
    // if (channel_name[0] != '/') {
    //     channel_name = "/" + channel_name;
    // }

    if (subscriptions.find(channel_name) == subscriptions.end()) {
        std::cerr << "Error: You are not subscribed to channel [" << channel_name << "]" << std::endl;
        
        // Print all subscribed channels for debugging
        std::cerr << "Currently subscribed channels: ";
        for (const auto& sub : subscriptions) {
            std::cerr << sub.first << " ";
        }
        std::cerr << std::endl;
        
        return;
    }
    

    const std::vector<Event>& events = parsedData.events;
    for (const Event& event : events) {
         savedEvents[channel_name].push_back(event);  // ✅ Store in `savedEvents`

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

        std::string formattedChannel = "/" + channel_name;  // ✅ Ensure it has the correct STOMP format
        std::string frame = createSendFrame(formattedChannel, body.str());


        {
            std::lock_guard<std::mutex> lock(connectionMutex);
            if (!connectionHandler->sendFrameAscii(frame, '\0')) {
                std::cerr << "Failed to send event to channel: " << channel_name << std::endl;
                return;
            }
        }
    }

    std::cout << "Reported" << std::endl;
}



void StompProtocol::handleSummaryCommand(const std::string& channel, const std::string& user, const std::string& filePath) {
    std::ofstream outFile(filePath, std::ios::trunc);
    if (!outFile.is_open()) {
        std::cerr << "Error: Could not create file " << filePath << std::endl;
        return;
    } else {
        std::cout << "File " << filePath << " created successfully." << std::endl;
    }
    

    // Check if the channel exists in savedEvents
    auto channelIt = savedEvents.find(channel);
    if (channelIt == savedEvents.end()) {
        outFile << "No events found for channel: " << channel << "\n";
        outFile.close();
        return;
    }

    const std::vector<Event>& events = channelIt->second;

    // Filter events for the specific user
    std::vector<Event> filteredEvents;
    std::copy_if(events.begin(), events.end(), std::back_inserter(filteredEvents),
                 [&user](const Event& event) { return event.getEventOwnerUser() == user; });

    if (filteredEvents.empty()) {
        outFile << "No events found from user: " << user << " on channel: " << channel << "\n";
        outFile.close();
        return;
    }

    // Sort by date time, then by event name
    std::sort(filteredEvents.begin(), filteredEvents.end(),
        [](const Event& a, const Event& b) {
        if (a.get_date_time() != b.get_date_time()) {
            return a.get_date_time() < b.get_date_time();
        }
        return a.get_name() < b.get_name();
    });

    int totalReports = filteredEvents.size();
    int activeCount = 0;
    int forcesArrivalCount = 0;

    for (const Event& event : filteredEvents) {
        const auto& generalInfo = event.get_general_information();
        if (generalInfo.count("active") && generalInfo.at("active") == "true") {
            activeCount++;
        }
        if (generalInfo.count("forces_arrival_at_scene") && generalInfo.at("forces_arrival_at_scene") == "true") {
            forcesArrivalCount++;
        }
    }

    // Write summary to file
    outFile << "Channel " << channel << "\n"
            << "Stats:\n"
            << "Total: " << totalReports << "\n"
            << "active: " << activeCount << "\n"
            << "forces arrival at scene: " << forcesArrivalCount << "\n\n";

    outFile << "Event Reports:\n";
    for (size_t i = 0; i < filteredEvents.size(); i++) {
        const Event& event = filteredEvents[i];
        std::string summary = event.get_description().substr(0, 27);
        if (event.get_description().length() > 27) {
            summary += "...";
        }

        outFile << "Report_" << (i+1) << ":\n"
                << "\tcity: " << event.get_city() << "\n"
                << "\tdate time: " << epoch_to_date(event.get_date_time()) << "\n"
                << "\tevent name: " << event.get_name() << "\n"
                << "\tsummary: " << summary << "\n\n";
    }

    outFile.close();
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