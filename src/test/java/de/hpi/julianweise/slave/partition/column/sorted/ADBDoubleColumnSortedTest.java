package de.hpi.julianweise.slave.partition.column.sorted;


import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBDoubleColumnSortedTest {

    private final static int NODE_ID = 1;
    private final static int PARTITION_ID = 0;

    @Test
    public void shouldReturnCorrectMaterializedEntries() throws IllegalAccessException {
        double[] values = new double[]{1, 2, 3};
        int[] originalIds = new int[]{1, 0, 2};
        ADBDoubleColumnSorted columnSorted = new ADBDoubleColumnSorted(NODE_ID, PARTITION_ID, values, originalIds);

        ObjectList<ADBEntityEntry> sortedEntries = columnSorted.materializeSorted();

        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(0).getId())).isEqualTo(1);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(1).getId())).isEqualTo(0);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(2).getId())).isEqualTo(2);

        assertThat(sortedEntries.get(0).getValueField().getDouble(sortedEntries.get(0))).isEqualTo(1);
        assertThat(sortedEntries.get(1).getValueField().getDouble(sortedEntries.get(1))).isEqualTo(2);
        assertThat(sortedEntries.get(2).getValueField().getDouble(sortedEntries.get(2))).isEqualTo(3);
    }

}