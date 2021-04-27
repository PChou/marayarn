package com.eoi.marayarn;

public class ResourceLimitException extends Exception {
    public ResourceLimitException(ResourceLimitType type, int capability, int needed) {
        super(String.format("%s: %d needed but the capacity only %d", type, needed, capability));
    }

    public enum ResourceLimitType {
        MEMORY("memory"), CORE("cpu");

        final String alias;
        ResourceLimitType(String alias) {
            this.alias = alias;
        }

        @Override
        public String toString() {
            return alias;
        }
    }
}
