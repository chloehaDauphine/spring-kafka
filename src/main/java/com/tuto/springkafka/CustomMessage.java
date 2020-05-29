package com.tuto.springkafka;

import lombok.Data;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Data
public class CustomMessage {
    private String itemRef;
    private String messageType;

    public CustomMessage() { }

    public CustomMessage(String itemRef, String messageType) {
        this.itemRef = itemRef;
        this.messageType = messageType;
    }

    public String getItemRef() {
        return this.itemRef;
    }

    public String getMessageType() {
        return this.messageType;
    }

    public void setItemRef(String itemRef) {
        this.itemRef = itemRef;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    @Override
    public String toString() {
        return "[itemRef: " + this.itemRef + ", messageType: " + this.messageType + "]";
    }
}
