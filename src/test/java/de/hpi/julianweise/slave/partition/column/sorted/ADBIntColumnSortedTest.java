package de.hpi.julianweise.slave.partition.column.sorted;


import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBIntColumnSortedTest {

    private final static int NODE_ID = 1;
    private final static int PARTITION_ID = 0;

    @Test
    public void shouldReturnCorrectMaterializedEntries() throws IllegalAccessException {
        int[] values = new int[]{1, 2, 3};
        short[] originalIds = new short[]{1, 0, 2};
        ADBIntColumnSorted columnSorted = new ADBIntColumnSorted(NODE_ID, PARTITION_ID, values, originalIds);

        ObjectList<ADBEntityEntry> sortedEntries = columnSorted.materializeSorted();

        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(0).getId())).isEqualTo(1);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(1).getId())).isEqualTo(0);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(2).getId())).isEqualTo(2);

        assertThat(sortedEntries.get(0).getValueField().getInt(sortedEntries.get(0))).isEqualTo(1);
        assertThat(sortedEntries.get(1).getValueField().getInt(sortedEntries.get(1))).isEqualTo(2);
        assertThat(sortedEntries.get(2).getValueField().getInt(sortedEntries.get(2))).isEqualTo(3);
    }

}