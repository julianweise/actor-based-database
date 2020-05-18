package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;

import java.util.List;

@FunctionalInterface
public interface ADBJoinTermCostCalculator {
    ADBInterval[] calc(List<ADBComparable2IntPair> left, List<ADBComparable2IntPair> right);
}
