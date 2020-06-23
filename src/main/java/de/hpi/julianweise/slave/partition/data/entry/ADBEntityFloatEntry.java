package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.data.entry.ADBEntityTypeEntry;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@SuppressWarnings("unused")
@NoArgsConstructor
@ADBEntityTypeEntry(valueType = float.class)
public class ADBEntityFloatEntry extends ADBEntityEntry {

    public float value;

    private transient static Field valueField;

    static {
        try {
            valueField = ADBEntityFloatEntry.class.getDeclaredField("value");
            valueField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    @SneakyThrows
    public ADBEntityFloatEntry(int id, Field field, ADBEntity entity) {
        super(id, field, entity);
        this.value = field.getFloat(entity);
    }

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBEntityFloatEntry.valueField;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + Integer.hashCode(this.getId());
        result = prime * result + Float.hashCode(this.value);
        return result;
    }

    @Override
    public String toString() {
        return "ADBEntityFloatEntry: " + this.value;
    }
}
