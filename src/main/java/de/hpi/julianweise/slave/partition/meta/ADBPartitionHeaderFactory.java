package de.hpi.julianweise.slave.partition.meta;

import de.hpi.julianweise.slave.partition.column.pax.ADBColumn;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import lombok.val;

import java.util.HashMap;
import java.util.Map;

public class ADBPartitionHeaderFactory {

    public static ADBPartitionHeader createDefault(Map<String, ADBColumn> columns, int id) {
        if (columns.size() < 1) {
            return new ADBPartitionHeader(new HashMap<>(), new HashMap<>(), id);
        }
        val minValues = ADBPartitionHeaderFactory.getMinValuesPerField(columns);
        val maxValues = ADBPartitionHeaderFactory.getMaxValuesPerField(columns);
        return new ADBPartitionHeader(minValues, maxValues, id);
    }

    private static Map<String, ADBEntityEntry> getMinValuesPerField(Map<String, ADBColumn> columns) {
        Map<String, ADBEntityEntry> minValues = new HashMap<>(columns.size());
        for (Map.Entry<String, ADBColumn> entry : columns.entrySet()) {
            minValues.put(entry.getKey(), entry.getValue().getSmallest());
        }
        return minValues;
    }

    private static Map<String, ADBEntityEntry> getMaxValuesPerField(Map<String, ADBColumn> columns) {
        Map<String, ADBEntityEntry> maxValues = new HashMap<>(columns.size());
        for (Map.Entry<String, ADBColumn> entry : columns.entrySet()) {
            maxValues.put(entry.getKey(), entry.getValue().getLargest());
        }
        return maxValues;
    }

}
