package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.util.Arrays;

public class ADBJoinTermEqualityCostCalculator implements ADBJoinTermCostCalculator {

    @Override
    public ADBInterval[][] calc(ObjectList<ADBEntityEntry> left, ObjectList<ADBEntityEntry> right, ADBComparator comparator) {
        ADBInterval[][] resultSet = new ADBInterval[left.size()][0];

        int leftId = 0, rightId = 0;
        ADBComparator comparatorA = ADBComparator.getFor(comparator.getLeftSideField(), comparator.getLeftSideField());
        while(leftId < left.size() && rightId < right.size()) {
            if (leftId > 0 && comparatorA.compare(left.get(leftId - 1), left.get(leftId)) == 0) {
                resultSet[leftId] = resultSet[leftId-1];
                leftId++;
            }
            else if (comparator.compare(left.get(leftId), right.get(rightId)) < 0) {
                leftId++;
            }
            else if (comparator.compare(left.get(leftId), right.get(rightId)) == 0) {
                int intervalStart = rightId;
                while(rightId + 1 < right.size() && comparator.compare(left.get(leftId), right.get(rightId + 1)) == 0) rightId++;
                resultSet[leftId++] = new ADBInterval[] {new ADBInterval(intervalStart, rightId++)};
            }
            else if (comparator.compare(left.get(leftId), right.get(rightId)) > 0) {
                rightId++;
            }
        }
        return Arrays.copyOf(resultSet, leftId);
    }
}
