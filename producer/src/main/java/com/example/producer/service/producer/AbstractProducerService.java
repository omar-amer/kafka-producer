package com.example.producer.service.producer;

public abstract class AbstractProducerService {

    protected abstract boolean isActive();
    protected abstract void publish();

    public void sendMessage(){
        if (isActive()) {
            publish();
        }
    }
}
