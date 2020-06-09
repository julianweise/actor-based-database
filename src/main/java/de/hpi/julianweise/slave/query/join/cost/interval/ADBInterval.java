package de.hpi.julianweise.slave.query.join.cost.interval;

import lombok.Getter;

@Getter
public class ADBInterval {

    public static final ADBInterval NO_INTERSECTION = new ADBInterval(-1,-1);

    private final int start;
    private final int end;

    public ADBInterval(int start, int end) {
        assert start <= end : "Start has to be <= compared to end. Interval: [" + start + ", " + end + "]";
        this.start = start;
        this.end = end;
    }

    public int size() {
        if (this.equals(NO_INTERSECTION)) {
            return 0;
        }
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
