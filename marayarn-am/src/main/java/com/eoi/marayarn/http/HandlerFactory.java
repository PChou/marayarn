package com.eoi.marayarn.http;

public interface HandlerFactory {
    /**
     *
     * @return
     */
    Iterable<Handler> getHandlers();
}
