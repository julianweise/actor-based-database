package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.data.entry.ADBEntityTypeEntry;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@SuppressWarnings("unused")
@NoArgsConstructor
@ADBEntityTypeEntry(valueType = int.class)
public class ADBEntityIntEntry extends ADBEntityEntry {

    public transient static Field valueField;

    static {
        try {
            valueField = ADBEntityIntEntry.class.getDeclaredField("value");
            valueField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    public int value;

    @SneakyThrows
    public ADBEntityIntEntry(int id, Field field, ADBEntity entity) {
        super(id, field, entity);
        this.value = field.getInt(entity);
    }

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBEntityIntEntry.valueField;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + Integer.hashCode(this.getId());
        result = prime * result + Integer.hashCode(this.value);
        return result;
    }

    @Override
    public String toString() {
        return "ADBEntityIntEntry: " + this.value;
    }
}
