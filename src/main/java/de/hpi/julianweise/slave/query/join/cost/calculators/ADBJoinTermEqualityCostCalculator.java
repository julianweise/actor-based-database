package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class ADBJoinTermEqualityCostCalculator implements ADBJoinTermCostCalculator {

    @Override
    public ADBInterval[][] calc(ObjectList<ADBEntityEntry> left, ObjectList<ADBEntityEntry> right) {
        ADBInterval[][] resultSet = new ADBInterval[left.size()][1];

        int a = 0, b = 0;
        ADBComparator comparator = null;
        ADBComparator comparatorA = null;
        if (left.size() > 0 && right.size() > 0) {
            comparator = ADBComparator.getFor(left.get(a).getValueField(), right.get(b).getValueField());
        }
        if (left.size() > 0) {
            comparatorA = ADBComparator.getFor(left.get(a).getValueField(), left.get(a).getValueField());
        }
        while(a < left.size() && b < right.size()) {
            // TODO: Find a more memory-efficient solution as the following condition may often match
            if (a > 0 && comparatorA.compare(left.get(a - 1), left.get(a)) == 0) {
                resultSet[a] = resultSet[a-1];
                a++;
                continue;
            }
            resultSet[a][0] = ADBInterval.NO_INTERSECTION;
            if (comparator.compare(left.get(a), right.get(b)) < 0) {
                a++;
                continue;
            }
            if (comparator.compare(left.get(a), right.get(b)) == 0) {
                int end = b;
                while(end + 1 < right.size() && comparator.compare(left.get(a), right.get(end + 1)) == 0) end++;
                resultSet[a++][0] = new ADBInterval(b, end);
                b = end + 1;
                continue;
            }
            if (comparator.compare(left.get(a), right.get(b)) > 0) {
                b++;
            }
        }
        while (a < left.size() && left.size() > 0) {
            if (a > 0 && comparatorA.compare(left.get(a - 1), left.get(a)) == 0) {
                resultSet[a] = resultSet[a-1];
                a++;
                continue;
            }
            resultSet[a++][0] = ADBInterval.NO_INTERSECTION;
        }
        return resultSet;
    }
}
