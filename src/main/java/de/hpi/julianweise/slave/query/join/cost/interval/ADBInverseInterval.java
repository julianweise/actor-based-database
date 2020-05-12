package de.hpi.julianweise.slave.query.join.cost.interval;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ADBInverseInterval implements ADBInterval {

    public static final ADBInterval NO_INTERSECTION = new ADBInverseInterval(-1,-1, -1);

    private final int start;
    private final int end;
    private final int referenceEnd;

    @Override
    public int size() {
        assert this.start <= this.end : "Start of an interval has to be <= compared to its end";
        assert this.end <= this.referenceEnd : "End of interval has to be <= compared to its underlying range";
        if (this.start == 0 && this.end == this.referenceEnd) {
            return 0;
        }
        if (this.start == 0) {
            return this.referenceEnd - this.end + 1;
        }
        if (this.end == this.referenceEnd) {
            return this.start + 1;
        }
        if (this.start == -1 && this.end == -1) {
            return this.referenceEnd + 1;
        }
        return this.start + 1 + this.referenceEnd - this.end + 1;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ADBInverseInterval)) {
            return false;
        }
        ADBInverseInterval oC = ((ADBInverseInterval) o);
        if (oC.referenceEnd == -1 && oC.start == -1 && oC.end == -1 && this.start == 0 && this.end == this.referenceEnd) {
            return true;
        }
        if (oC.referenceEnd == -1 && this.referenceEnd == -1) {
            return true;
        }
        return this.start == oC.start && this.end == oC.end && this.referenceEnd == oC.referenceEnd;
    }

    @Override
    public String toString() {
        if (this.equals(NO_INTERSECTION) || this.start == 0 && this.end == this.referenceEnd || referenceEnd == -1) {
            return "[]";
        }
        if (this.start == 0) {
            return "[" + (this.end + 1) + ", " + this.referenceEnd + "]";
        }
        if (this.end == this.referenceEnd) {
            return "[0, " + (this.start - 1) + "]";
        }
        if (this.start == -1 && this.end == -1) {
            return "[0, " + this.referenceEnd + "]";
        }
        return "[0, " + (this.start - 1) + "], [" + (this.end + 1) + ", " + this.referenceEnd + "]";
    }
}
