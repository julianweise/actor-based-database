package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import it.unimi.dsi.fastutil.objects.ObjectList;

@FunctionalInterface
public interface ADBJoinTermCostCalculator {
    ADBInterval[][] calc(ObjectList<ADBEntityEntry> left, ObjectList<ADBEntityEntry> right);
}
