package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBIntervalImpl;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class ADBJoinTermLessOrEqualCostCalculator implements ADBJoinTermCostCalculator {

    @Override
    public ADBInterval[] calc(ObjectList<ADBComparable2IntPair> left, ObjectList<ADBComparable2IntPair> right) {
        ADBInterval[] resultSet = new ADBIntervalImpl[left.size()];

        int a = 0, b = 0;
        while(a < left.size() && b < right.size()) {
            resultSet[a] = ADBIntervalImpl.NO_INTERSECTION;
            if (left.get(a).getKey().compareTo(right.get(b).getKey()) <= 0) {
                resultSet[a++] = new ADBIntervalImpl(b, right.size() - 1);
                continue;
            }
            if (left.get(a).getKey().compareTo(right.get(b).getKey()) > 0) {
                b++;
            }
        }
        while (a < left.size()) resultSet[a++] = ADBIntervalImpl.NO_INTERSECTION;
        return resultSet;
    }
}
