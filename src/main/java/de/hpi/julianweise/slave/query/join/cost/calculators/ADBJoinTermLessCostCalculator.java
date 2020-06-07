package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class ADBJoinTermLessCostCalculator implements ADBJoinTermCostCalculator {

    @Override
    public ADBInterval[][] calc(ObjectList<ADBEntityEntry> left, ObjectList<ADBEntityEntry> right, ADBComparator comparator) {
        ADBInterval[][] resultSet = new ADBInterval[left.size()][1];
        int leftIndex = 0, rightIndex = 0;
        while(leftIndex < left.size() && rightIndex < right.size()) {
            resultSet[leftIndex][0] = ADBInterval.NO_INTERSECTION;
            if (comparator.compare(left.get(leftIndex), right.get(rightIndex)) < 0) {
                resultSet[leftIndex++][0] = new ADBInterval(rightIndex, right.size() - 1);
            }
            else if (comparator.compare(left.get(leftIndex), right.get(rightIndex)) >= 0) {
                rightIndex++;
            }
        }
        for (;leftIndex < left.size(); leftIndex++) resultSet[leftIndex][0] = ADBInterval.NO_INTERSECTION;
        return resultSet;
    }
}
