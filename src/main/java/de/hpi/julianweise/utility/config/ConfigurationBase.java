package de.hpi.julianweise.utility.config;

public interface ConfigurationBase {
    enum OperationRole {
        MASTER, SLAVE
    }

    OperationRole role();

    int getPort();
}
