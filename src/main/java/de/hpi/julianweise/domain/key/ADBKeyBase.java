package de.hpi.julianweise.domain.key;

public interface ADBKeyBase {
    boolean equals(Object other);
    int hashCode();
    String toString();
}
