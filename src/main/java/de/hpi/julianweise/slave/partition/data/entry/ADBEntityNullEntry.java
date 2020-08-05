package de.hpi.julianweise.slave.partition.data.entry;

import java.lang.reflect.Field;

public class ADBEntityNullEntry implements ADBEntityEntry {

    private static final ADBEntityNullEntry INSTANCE = new ADBEntityNullEntry();

    public static ADBEntityNullEntry getInstance() {
        return INSTANCE;
    }

    private ADBEntityNullEntry() {}

    @Override
    public int getId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Field getValueField() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull() {
        return true;
    }
}
