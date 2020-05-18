package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInverseInterval;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;

import java.util.Arrays;
import java.util.List;

public class ADBJoinTermInequalityCostCalculator implements ADBJoinTermCostCalculator {

    @Override
    public ADBInterval[] calc(List<ADBComparable2IntPair> left,
                              List<ADBComparable2IntPair> right) {
        ADBInterval[] resultSet = new ADBJoinTermEqualityCostCalculator().calc(left, right);
        return Arrays.stream(resultSet)
                     .map(interval -> new ADBInverseInterval(interval.getStart(), interval.getEnd(), right.size() - 1))
                     .toArray(ADBInverseInterval[]::new);
    }
}
