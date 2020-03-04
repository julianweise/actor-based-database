package de.hpi.julianweise.domain.key;

public interface ADBKey {

    int compareTo(ADBKey other) throws ADBKeyComparisonException;
    boolean equals(Object other);
    int hashCode();
    String toString();
}
