package de.hpi.julianweise.slave.partition.data.entry;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.data.entry.ADBEntityTypeEntry;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Field;

@SuppressWarnings("unused")
@NoArgsConstructor
@ADBEntityTypeEntry(valueType = byte.class)
public class ADBEntityByteEntry extends ADBEntityEntry {

    private transient static Field valueField;

    static {
        try {
            valueField = ADBEntityByteEntry.class.getDeclaredField("value");
            valueField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    public byte value;

    @SneakyThrows
    public ADBEntityByteEntry(int id, Field field, ADBEntity entity) {
        super(id, field, entity);
        this.value = field.getByte(entity);
    }

    @Override
    @SneakyThrows
    public Field getValueField() {
        return ADBEntityByteEntry.valueField;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + Integer.hashCode(this.getId());
        result = prime * result + Byte.hashCode(this.value);
        return result;
    }
}
