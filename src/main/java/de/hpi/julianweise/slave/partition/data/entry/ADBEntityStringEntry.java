package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.data.entry.ADBEntityTypeEntry;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@SuppressWarnings("unused")
@NoArgsConstructor
@ADBEntityTypeEntry(valueType = String.class)
public class ADBEntityStringEntry implements ADBEntityEntry {

    private static transient Field valueField;

    static {
        try {
            valueField = ADBEntityStringEntry.class.getDeclaredField("value");
            valueField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    @Getter
    public int id;
    public String value;

    @SneakyThrows
    public ADBEntityStringEntry(int id, Field field, ADBEntity entity) {
        this.id = id;
        this.value = (String) field.get(entity);
    }

    public ADBEntityStringEntry(int id, String value) {
        this.id = id;
        this.value = value;
    }


    @SneakyThrows
    public Field getValueField() {
        return ADBEntityStringEntry.valueField;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + Integer.hashCode(this.getId());
        result = prime * result + this.value.hashCode();
        return result;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public String toString() {
        return "ADBEntityStringEntry: " + this.value;
    }
}
