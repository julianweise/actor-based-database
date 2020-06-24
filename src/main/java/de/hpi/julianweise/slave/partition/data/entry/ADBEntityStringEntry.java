package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.data.entry.ADBEntityTypeEntry;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@SuppressWarnings("unused")
@NoArgsConstructor
@ADBEntityTypeEntry(valueType = String.class)
public class ADBEntityStringEntry extends ADBEntityEntry {

    private static transient Field valueField;

    static {
        try {
            valueField = ADBEntityStringEntry.class.getDeclaredField("value");
            valueField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    public String value;

    @SneakyThrows
    public ADBEntityStringEntry(int id, Field field, ADBEntity entity) {
        super(id, field, entity);
        this.value = (String) field.get(entity);
    }

    @Override
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
    public String toString() {
        return "ADBEntityStringEntry: " + this.value;
    }
}
