package de.hpi.julianweise;

import de.hpi.julianweise.slave.partition.data.ADBEntityFactory;
import de.hpi.julianweise.domain.custom.SFEmployeeSalary.SFEmployeeFactory;

public class ADBQuickStart {
    public static void main(String[] args) {
        ADBEntityFactory entityFactory = ADBQuickStart.createEntityFactory();
        ADBApplication database = new ADBApplication(entityFactory);
        database.run(args);
    }

    private static ADBEntityFactory createEntityFactory() {
        return new SFEmployeeFactory();
    }
}
