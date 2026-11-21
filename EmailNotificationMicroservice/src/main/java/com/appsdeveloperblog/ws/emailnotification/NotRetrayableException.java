package com.appsdeveloperblog.ws.emailnotification;

public class NotRetrayableException extends RuntimeException {

    public NotRetrayableException(String message) {
        super(message);
    }

    public NotRetrayableException(Throwable cause) {
        super(cause);
    }
    
}
