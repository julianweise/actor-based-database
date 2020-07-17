package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class ADBJoinTermLessCostCalculator implements ADBJoinTermCostCalculator {

    @Override
    public ADBInterval[][] calc(ObjectList<ADBEntityEntry> left, ObjectList<ADBEntityEntry> right, ADBComparator comparator) {
        ADBInterval[][] resultSet = new ADBInterval[left.size()][0];
        int leftIndex = 0, rightIndex = 0;
        while(leftIndex < left.size() && rightIndex < right.size()) {
            if (comparator.compare(left.get(leftIndex), right.get(rightIndex)) < 0) {
                resultSet[leftIndex++] = new ADBInterval[] {new ADBInterval(rightIndex, right.size() - 1)};
            }
            else if (comparator.compare(left.get(leftIndex), right.get(rightIndex)) >= 0) {
                rightIndex++;
            }
        }
        return resultSet;
    }
}
