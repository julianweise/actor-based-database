package de.hpi.julianweise.domain.key;

public interface ADBKey extends Comparable<ADBKey> {
    boolean equals(Object other);
    int hashCode();
    String toString();
}
