package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.data.entry.ADBEntityTypeEntry;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@SuppressWarnings("unused")
@NoArgsConstructor
@ADBEntityTypeEntry(valueType = double.class)
public class ADBEntityDoubleEntry implements ADBEntityEntry {

    private transient static Field valueField;

    static {
        try {
            valueField = ADBEntityDoubleEntry.class.getDeclaredField("value");
            valueField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    @Getter
    public int id;
    public double value;

    @SneakyThrows
    public ADBEntityDoubleEntry(int id, Field field, ADBEntity entity) {
        this.id = id;
        this.value = field.getDouble(entity);
    }

    public ADBEntityDoubleEntry(int id, double value) {
        this.id = id;
        this.value = value;
    }


    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBEntityDoubleEntry.valueField;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + Integer.hashCode(this.getId());
        result = prime * result + Double.hashCode(this.value);
        return result;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public String toString() {
        return "ADBEntityDoubleEntry: " + this.value;
    }
}
