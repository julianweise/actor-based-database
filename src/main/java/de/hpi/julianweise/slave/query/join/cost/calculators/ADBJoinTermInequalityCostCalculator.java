package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class ADBJoinTermInequalityCostCalculator implements ADBJoinTermCostCalculator {

    @Override
    public ADBInterval[][] calc(ObjectList<ADBEntityEntry> left, ObjectList<ADBEntityEntry> right, ADBComparator comparator) {
        ADBInterval[][] resultSet = new ADBInterval[left.size()][2];

        int leftIndex = 0, rightIndex = 0;
        ADBComparator comparatorA = ADBComparator.getFor(comparator.getLeftSideField(), comparator.getLeftSideField());
        while(leftIndex < left.size() && rightIndex < right.size()) {
            if (leftIndex > 0 && comparatorA.compare(left.get(leftIndex - 1), left.get(leftIndex)) == 0) {
                resultSet[leftIndex] = resultSet[leftIndex-1];
                leftIndex++;
                continue;
            }
            resultSet[leftIndex][0] = new ADBInterval(0, right.size() - 1);
            resultSet[leftIndex][1] = ADBInterval.NO_INTERSECTION;
            if (comparator.compare(left.get(leftIndex), right.get(rightIndex)) < 0) {
                leftIndex++;
            }
            else if (comparator.compare(left.get(leftIndex), right.get(rightIndex)) == 0) {
                int intervalStart = rightIndex;
                while(rightIndex + 1 < right.size() && comparator.compare(left.get(leftIndex), right.get(rightIndex + 1)) == 0) rightIndex++;
                resultSet[leftIndex][0] = rightIndex > 0 ? new ADBInterval(0, intervalStart - 1) : ADBInterval.NO_INTERSECTION;
                resultSet[leftIndex++][1] = rightIndex < (right.size() - 1) ? new ADBInterval(rightIndex + 1, right.size() - 1) : ADBInterval.NO_INTERSECTION;
                rightIndex++;
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
            resultSet[leftIndex][0] = right.size() > 0 ? new ADBInterval(0, right.size() - 1) : ADBInterval.NO_INTERSECTION;
            resultSet[leftIndex][1] = ADBInterval.NO_INTERSECTION;
        }
        return resultSet;
    }
}
