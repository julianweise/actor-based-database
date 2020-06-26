package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.data.entry.ADBEntityTypeEntry;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@SuppressWarnings("unused")
@NoArgsConstructor
@ADBEntityTypeEntry(valueType = int.class)
public class ADBEntityIntEntry implements ADBEntityEntry {

    public transient static Field valueField;

    static {
        try {
            valueField = ADBEntityIntEntry.class.getDeclaredField("value");
            valueField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    @Getter
    private int id;

    @Getter
    private int value;

    @SneakyThrows
    public ADBEntityIntEntry(int id, Field field, ADBEntity entity) {
        this.id = id;
        this.value = field.getInt(entity);
    }

    public ADBEntityIntEntry(int id, int value) {
        this.id = id;
        this.value = value;
    }

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
