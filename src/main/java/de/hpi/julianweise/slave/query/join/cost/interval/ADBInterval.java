package de.hpi.julianweise.slave.query.join.cost.interval;

public interface ADBInterval {

    int getStart();
    int getEnd();
    int size();
    boolean equals(Object o);
    String toString();
}
