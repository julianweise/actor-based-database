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
    public void shouldReturnCorrectMaterializedEntries() {
        int[] values = new int[]{1, 2, 3, 4, 5}; // 4, 1, 2, 3, 5
        int[] sortedToOriginal = new int[]{1, 2, 3, 0, 4};
        int[] originalToSorted = new int[]{3, 0, 1, 2, 4};
        ADBIntColumnSorted columnSorted = new ADBIntColumnSorted(NODE_ID, PARTITION_ID, values, sortedToOriginal, originalToSorted);

        ObjectList<ADBEntityEntry> sortedEntries = columnSorted.materializeSorted();

        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(0).getId())).isEqualTo(1);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(1).getId())).isEqualTo(2);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(2).getId())).isEqualTo(3);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(3).getId())).isEqualTo(0);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(4).getId())).isEqualTo(4);
    }

}