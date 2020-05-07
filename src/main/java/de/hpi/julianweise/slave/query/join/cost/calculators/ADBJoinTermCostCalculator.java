package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.utility.largemessage.ADBPair;

import java.util.List;

@FunctionalInterface
public interface ADBJoinTermCostCalculator {
    ADBInterval[] calc(List<ADBPair<Comparable<Object>, Integer>> left, List<ADBPair<Comparable<Object>, Integer>> right);
}
