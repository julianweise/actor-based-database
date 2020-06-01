package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBIntervalImpl;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class ADBJoinTermGreaterCostCalculator implements ADBJoinTermCostCalculator {

    @Override
    public ADBInterval[] calc(ObjectList<ADBEntityEntry> left, ObjectList<ADBEntityEntry> right) {
        ADBInterval[] resultSet = new ADBIntervalImpl[left.size()];
        int a = 0, b = 0;
        ADBComparator comparator = null;
        if (left.size() > 0 && right.size() > 0) {
            comparator = ADBComparator.getFor(left.get(a).getValueField(), right.get(b).getValueField());
        }
        while(a < left.size() && b < right.size()) {
            resultSet[a] = ADBIntervalImpl.NO_INTERSECTION;
            if (comparator.compare(left.get(a), right.get(b)) <= 0) {
                a++;
                continue;
            }
            if (comparator.compare(left.get(a), right.get(b)) > 0) {
                while(b + 1 < right.size() && comparator.compare(left.get(a), right.get(b + 1)) > 0) b++;
                resultSet[a++] = new ADBIntervalImpl(0, b);
            }
        }
        while (a < left.size()) resultSet[a++] = ADBIntervalImpl.NO_INTERSECTION;
        return resultSet;
    }
}
