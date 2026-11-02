package com.example.products.rest;

import java.util.Date;

public class ErrorMessage {
    private String message;
    private String details;
    private Date timestamp;

    public ErrorMessage(Date timestamp, String message, String details) {
        super();
        this.message = message;
        this.details = details;
        this.timestamp = timestamp;
    }

    public String getMessage() {
        return message;
    }

    public String getDetails() {
        return details;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

}
