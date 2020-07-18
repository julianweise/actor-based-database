package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.util.ArrayList;
import java.util.Arrays;

public class ADBJoinTermInequalityCostCalculator implements ADBJoinTermCostCalculator {

    @Override
    public ADBInterval[][] calc(ObjectList<ADBEntityEntry> left, ObjectList<ADBEntityEntry> right, ADBComparator comparator) {
        ADBInterval[][] resultSet = new ADBInterval[left.size()][0];

        int leftIndex = 0, rightIndex = 0;
        ADBComparator comparatorA = ADBComparator.getFor(comparator.getLeftSideField(), comparator.getLeftSideField());
        while(leftIndex < left.size() && rightIndex < right.size()) {
            if (leftIndex > 0 && comparatorA.compare(left.get(leftIndex - 1), left.get(leftIndex)) == 0) {
                resultSet[leftIndex] = resultSet[leftIndex-1];
                leftIndex++;
                continue;
            }
            resultSet[leftIndex] = new ADBInterval[] {new ADBInterval(0, right.size() - 1)};
            if (comparator.compare(left.get(leftIndex), right.get(rightIndex)) < 0) {
                leftIndex++;
            }
            else if (comparator.compare(left.get(leftIndex), right.get(rightIndex)) == 0) {
                int intervalStart = rightIndex;
                while(rightIndex + 1 < right.size() && comparator.compare(left.get(leftIndex), right.get(rightIndex + 1)) == 0) rightIndex++;
                resultSet[leftIndex] = this.handleEquality(intervalStart, rightIndex, right.size());
                rightIndex++; leftIndex++;
            }
            else if (comparator.compare(left.get(leftIndex), right.get(rightIndex)) > 0) {
                rightIndex++;
            }
        }
        for (;leftIndex < left.size(); leftIndex++) {
            if (leftIndex > 0 && comparatorA.compare(left.get(leftIndex - 1), left.get(leftIndex)) == 0) {
                resultSet[leftIndex] = resultSet[leftIndex-1];
                continue;
            }
            if (right.size() > 0) {
                resultSet[leftIndex] = new ADBInterval[] {new ADBInterval(0, right.size() - 1)};
            }
        }
        return Arrays.copyOf(resultSet, leftIndex);
    }

    private ADBInterval[] handleEquality(int equalityStart, int equalityEnd, int rangeSize) {
        ArrayList<ADBInterval> intervals = new ArrayList<>(2);
        if (equalityStart > 0) intervals.add(this.leftInterval(equalityStart));
        if (equalityEnd < (rangeSize - 1)) intervals.add(this.rightInterval(equalityEnd, rangeSize));
        intervals.trimToSize();
        return intervals.toArray(new ADBInterval[0]);
    }

    private ADBInterval leftInterval(int equalityIntervalStart) {
        return new ADBInterval(0, equalityIntervalStart - 1);
    }

    private ADBInterval rightInterval(int equalityIntervalEnd, int rangeSize) {
        return new ADBInterval(equalityIntervalEnd + 1, rangeSize - 1);
    }
}
