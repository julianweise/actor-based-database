package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.util.Arrays;

public class ADBJoinTermGreaterOrEqualCostCalculator implements ADBJoinTermCostCalculator {

    @Override
    public ADBInterval[][] calc(ObjectList<ADBEntityEntry> left, ObjectList<ADBEntityEntry> right, ADBComparator comparator) {
        ADBInterval[][] resultSet = new ADBInterval[left.size()][0];
        int leftIndex = 0, rightIndex = 0, lastLeftEntry = -1;
        while(leftIndex < left.size() && rightIndex < right.size()) {
            if (comparator.compare(left.get(leftIndex), right.get(rightIndex)) < 0) {
                leftIndex++;
            }
            else if (comparator.compare(left.get(leftIndex), right.get(rightIndex)) >= 0) {
                while(rightIndex + 1 < right.size() && comparator.compare(left.get(leftIndex), right.get(rightIndex + 1)) >= 0) rightIndex++;
                lastLeftEntry = leftIndex;
                resultSet[leftIndex++] = new ADBInterval[] {new ADBInterval(0, rightIndex)};
            }
        }
        return Arrays.copyOf(resultSet, lastLeftEntry + 1);
    }
}
