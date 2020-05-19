package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import it.unimi.dsi.fastutil.objects.ObjectList;

@FunctionalInterface
public interface ADBJoinTermCostCalculator {
    ADBInterval[] calc(ObjectList<ADBComparable2IntPair> left, ObjectList<ADBComparable2IntPair> right);
}
