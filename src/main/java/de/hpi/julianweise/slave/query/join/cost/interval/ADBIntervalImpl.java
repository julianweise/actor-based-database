package de.hpi.julianweise.slave.query.join.cost.interval;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ADBIntervalImpl implements ADBInterval {

    public static final ADBInterval NO_INTERSECTION = new ADBIntervalImpl(-1,-1);

    private final int start;
    private final int end;

    public int size() {
        assert this.start <= this.end : "Start of an interval has to be <= compared to its end";
        if (this.equals(NO_INTERSECTION)) {
            return 0;
        }
        return 1 + this.end - this.start;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ADBIntervalImpl)) {
            return false;
        }
        return this.start == ((ADBIntervalImpl) o).start && this.end == ((ADBIntervalImpl) o).end;
    }

    @Override
    public String toString() {
        return "[" + this.start + ", " + this.end + "]";
    }
}
