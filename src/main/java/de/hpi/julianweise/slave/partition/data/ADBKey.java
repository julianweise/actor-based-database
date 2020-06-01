package de.hpi.julianweise.slave.partition.data;

public interface ADBKey extends Comparable<ADBKey> {
    boolean equals(Object other);

    int hashCode();

    String toString();
}
