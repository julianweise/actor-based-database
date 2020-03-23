package de.hpi.julianweise;

import de.hpi.julianweise.domain.ADBEntityFactory;
import de.hpi.julianweise.domain.custom.SFEmployeeFactory;

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
