package de.hpi.julianweise;

import de.hpi.julianweise.domain.custom.tpch.LineItemAndPartFactory;
import de.hpi.julianweise.slave.partition.data.ADBEntityFactory;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;

public class ADBQuickStart {
    public static void main(String[] args) {
        ADBEntityFactory entityFactory = ADBQuickStart.createEntityFactory();
        ADBComparator.buildComparatorMapping();
        ADBApplication database = new ADBApplication(entityFactory);
        database.run(args);
    }

    private static ADBEntityFactory createEntityFactory() {
        return new LineItemAndPartFactory();
    }
}
