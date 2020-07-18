package de.hpi.julianweise.slave.query.join.cost.interval;

import lombok.Getter;

@Getter
public class ADBInterval {

    private final int start;
    private final int end;

    public ADBInterval(int start, int end) {
        assert start <= end : "Start has to be <= compared to end. Interval: [" + start + ", " + end + "]";
        this.start = start;
        this.end = end;
    }

    public int size() {
        return 1 + this.end - this.start;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ADBInterval)) {
            return false;
        }
        return this.start == ((ADBInterval) o).start && this.end == ((ADBInterval) o).end;
    }

    @Override
    public String toString() {
        return "[" + this.start + ", " + this.end + "]";
    }
}
