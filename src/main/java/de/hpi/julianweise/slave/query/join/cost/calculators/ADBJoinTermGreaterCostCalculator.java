package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class ADBJoinTermGreaterCostCalculator implements ADBJoinTermCostCalculator {

    @Override
    public ADBInterval[][] calc(ObjectList<ADBEntityEntry> left, ObjectList<ADBEntityEntry> right, ADBComparator comparator) {
        ADBInterval[][] resultSet = new ADBInterval[left.size()][1];
        int leftId = 0, rightId = 0;
        while(leftId < left.size() && rightId < right.size()) {
            resultSet[leftId][0] = ADBInterval.NO_INTERSECTION;
            if (comparator.compare(left.get(leftId), right.get(rightId)) <= 0) {
                leftId++;
            }
            else if (comparator.compare(left.get(leftId), right.get(rightId)) > 0) {
                while(rightId + 1 < right.size() && comparator.compare(left.get(leftId), right.get(rightId + 1)) > 0) rightId++;
                resultSet[leftId++][0] = new ADBInterval(0, rightId);
            }
        }
        for (;leftId < left.size(); leftId++) resultSet[leftId][0] = ADBInterval.NO_INTERSECTION;
        return resultSet;
    }
}
