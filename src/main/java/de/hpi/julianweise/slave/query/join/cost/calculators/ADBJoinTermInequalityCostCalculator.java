package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBIntervalImpl;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInverseInterval;
import de.hpi.julianweise.utility.largemessage.ADBPair;

import java.util.Arrays;
import java.util.List;

public class ADBJoinTermInequalityCostCalculator implements ADBJoinTermCostCalculator {

    @Override
    public ADBInterval[] calc(List<ADBPair<Comparable<Object>, Integer>> left,
                              List<ADBPair<Comparable<Object>, Integer>> right) {
        ADBInterval[] resultSet = new ADBJoinTermEqualityCostCalculator().calc(left, right);
        return Arrays.stream(resultSet)
                     .map(interval -> new ADBInverseInterval(interval.getStart(), interval.getEnd(), right.size() - 1))
                     .toArray(ADBInverseInterval[]::new);
    }
}
