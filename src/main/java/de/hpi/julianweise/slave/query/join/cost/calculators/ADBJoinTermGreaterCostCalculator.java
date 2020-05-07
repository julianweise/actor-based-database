package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBIntervalImpl;
import de.hpi.julianweise.utility.largemessage.ADBPair;

import java.util.List;

public class ADBJoinTermGreaterCostCalculator implements ADBJoinTermCostCalculator {

    @Override
    public ADBInterval[] calc(List<ADBPair<Comparable<Object>, Integer>> left, List<ADBPair<Comparable<Object>, Integer>> right) {
        ADBInterval[] resultSet = new ADBIntervalImpl[left.size()];

        int a = 0, b = 0;
        while(a < left.size() && b < right.size()) {
            resultSet[a] = ADBIntervalImpl.NO_INTERSECTION;
            if (left.get(a).getKey().compareTo(right.get(b).getKey()) <= 0) {
                a++;
                continue;
            }
            if (left.get(a).getKey().compareTo(right.get(b).getKey()) > 0) {
                while(b + 1 < right.size() && left.get(a).getKey().compareTo(right.get(b + 1).getKey()) > 0) b++;
                resultSet[a++] = new ADBIntervalImpl(0, b);
            }
        }
        while (a < left.size()) resultSet[a++] = ADBIntervalImpl.NO_INTERSECTION;
        return resultSet;
    }
}
