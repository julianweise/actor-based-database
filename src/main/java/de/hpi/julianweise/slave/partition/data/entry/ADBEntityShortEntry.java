package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.data.entry.ADBEntityTypeEntry;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@SuppressWarnings("unused")
@NoArgsConstructor
@ADBEntityTypeEntry(valueType = short.class)
public class ADBEntityShortEntry implements ADBEntityEntry {


    private transient static Field valueField;
    static {
        try {
            valueField = ADBEntityShortEntry.class.getDeclaredField("value");
            valueField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    @Getter
    public int id;
    public short value;

    @SneakyThrows
    public ADBEntityShortEntry(int id, Field field, ADBEntity entity) {
        this.id = id;
        this.value = field.getShort(entity);
    }

    public ADBEntityShortEntry(int id, short value) {
        this.id = id;
        this.value = value;
    }


    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBEntityShortEntry.valueField;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + Integer.hashCode(this.getId());
        result = prime * result + Short.hashCode(this.value);
        return result;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public String toString() {
        return "ADBEntityShortEntry: " + this.value;
    }
}
