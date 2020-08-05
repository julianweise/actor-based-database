package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.data.entry.ADBEntityTypeEntry;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@SuppressWarnings("unused")
@NoArgsConstructor
@ADBEntityTypeEntry(valueType = boolean.class)
public class ADBEntityBooleanEntry implements ADBEntityEntry {

    private transient static Field valueField;

    static {
        try {
            valueField = ADBEntityBooleanEntry.class.getDeclaredField("value");
            valueField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    @Getter
    public int id;
    public boolean value;

    public ADBEntityBooleanEntry(int id, Field field, ADBEntity entity) {
        this.id = id;
        try {
            this.value = field.getBoolean(entity);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public ADBEntityBooleanEntry(int id, boolean value) {
        this.id = id;
        this.value = value;
    }

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBEntityBooleanEntry.valueField;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + Integer.hashCode(this.getId());
        result = prime * result + Boolean.hashCode(this.value);
        return result;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public String toString() {
        return "ADBEntityBooleanEntry: " + this.value;
    }
}
