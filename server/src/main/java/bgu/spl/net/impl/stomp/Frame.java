package bgu.spl.net.impl.stomp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Frame {
    private final String command;
    private final Map<String, String> headers;
    private final String body;

    public Frame(String command, Map<String, String> headers, String body) {
        this.command = command;
        this.headers = headers;
        this.body = body;
    }

    public String getCommand() {
        return command;
    }

    public String getHeader(String key) {
        return headers.getOrDefault(key, "");
    }

    public String getBody() {
        return body;
    }

    public static Frame fromRawMessage(List<String> parsedMessage) {
        if (parsedMessage.isEmpty()) {
            return new Frame("", new HashMap<>(), "");
        }

        String command = parsedMessage.get(0);
        Map<String, String> headers = new HashMap<>();
        StringBuilder body = new StringBuilder();
        boolean isBody = false;

        for (int i = 1; i < parsedMessage.size(); i++) {
            String line = parsedMessage.get(i);
            if (line.isEmpty()) {
                isBody = true;
                continue;
            }

            if (isBody) {
                body.append(line).append("\n");
            } else {
                String[] parts = line.split(":", 2);
                if (parts.length == 2) {
                    headers.put(parts[0].trim(), parts[1].trim());
                }
            }
        }

        return new Frame(command, headers, body.toString().trim());
    }
}
