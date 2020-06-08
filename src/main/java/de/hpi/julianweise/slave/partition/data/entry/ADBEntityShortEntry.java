package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.data.entry.ADBEntityTypeEntry;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@SuppressWarnings("unused")
@NoArgsConstructor
@ADBEntityTypeEntry(valueType = short.class)
public class ADBEntityShortEntry extends ADBEntityEntry {

    public short value;

    private transient static Field valueField;

    static {
        try {
            valueField = ADBEntityShortEntry.class.getDeclaredField("value");
            valueField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    @SneakyThrows
    public ADBEntityShortEntry(int id, Field field, ADBEntity entity) {
        super(id, field, entity);
        this.value = field.getShort(entity);
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
}