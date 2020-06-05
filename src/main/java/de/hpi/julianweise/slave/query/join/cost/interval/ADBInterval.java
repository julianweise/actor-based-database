package de.hpi.julianweise.slave.query.join.cost.interval;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ADBInterval {

    public static final ADBInterval NO_INTERSECTION = new ADBInterval(-1,-1);

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
