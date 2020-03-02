package de.hpi.julianweise.utility;

public interface ConfigurationBase {
    enum OperationRole {
        MASTER, SLAVE
    }

    OperationRole role();

    int getPort();
}
