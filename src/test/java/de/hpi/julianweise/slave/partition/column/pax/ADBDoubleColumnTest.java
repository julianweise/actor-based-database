package de.hpi.julianweise.slave.partition.column.pax;

import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBDoubleColumnSorted;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityDoubleEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBDoubleColumnTest {

    private final static int PARTITION_ID = 1;

    @Test
    public void shouldBeCreatedSuccessfully() throws NoSuchFieldException {
        Field field = TestEntity.class.getDeclaredField("eDouble");
        ADBDoubleColumn column = new ADBDoubleColumn(field, PARTITION_ID);

        TestEntity testEntity1 = new TestEntity(1, "Test1", 1f, true, 1);
        TestEntity testEntity2 = new TestEntity(2, "Test2", 2f, true, 2);
        TestEntity testEntity3 = new TestEntity(3, "Test3", 3f, true, 3);

        column.add(testEntity2);
        column.add(testEntity1);
        column.add(testEntity3);
        column.complete();

        assertThat(column.size()).isEqualTo(3);
        assertThat(column.partitionId).isEqualTo(1);
        assertThat(column.finalized).isTrue();
        assertThat(column.sortedIndices).isEqualTo(new short[]{1, 0, 2});
    }

    @Test
    public void shouldReturnCorrectSortedColumn() throws NoSuchFieldException, IllegalAccessException {
        Field field = TestEntity.class.getDeclaredField("eDouble");
        ADBDoubleColumn column = new ADBDoubleColumn(field, PARTITION_ID);

        TestEntity testEntity1 = new TestEntity(1, "Test1", 1f, true, 1);
        TestEntity testEntity2 = new TestEntity(2, "Test2", 2f, true, 2);
        TestEntity testEntity3 = new TestEntity(3, "Test3", 3f, true, 3);

        column.add(testEntity2);
        column.add(testEntity1);
        column.add(testEntity3);
        column.complete();

        ADBColumnSorted sortedColumn = column.getSortedColumn(new ADBEntityDoubleEntry(0, 1), new ADBEntityDoubleEntry(2, 3));
        assertThat(sortedColumn instanceof ADBDoubleColumnSorted).isTrue();

        ObjectList<ADBEntityEntry> sortedEntries = sortedColumn.materializeSorted();

        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(0).getId())).isEqualTo(1);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(1).getId())).isEqualTo(0);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(2).getId())).isEqualTo(2);

        assertThat(sortedEntries.get(0).getValueField().getDouble(sortedEntries.get(0))).isEqualTo(1);
        assertThat(sortedEntries.get(1).getValueField().getDouble(sortedEntries.get(1))).isEqualTo(2);
        assertThat(sortedEntries.get(2).getValueField().getDouble(sortedEntries.get(2))).isEqualTo(3);
    }

    @Test
    public void shouldReturnCorrectSortedColumnLimitedMax() throws NoSuchFieldException, IllegalAccessException {
        Field field = TestEntity.class.getDeclaredField("aInteger");
        ADBDoubleColumn column = new ADBDoubleColumn(field, PARTITION_ID);

        TestEntity testEntity1 = new TestEntity(1, "Test1", 1f, true, 1);
        TestEntity testEntity2 = new TestEntity(2, "Test2", 2f, true, 2);
        TestEntity testEntity3 = new TestEntity(3, "Test3", 3f, true, 3);
        TestEntity testEntity4 = new TestEntity(4, "Test4", 4f, true, 4);
        TestEntity testEntity5 = new TestEntity(5, "Test5", 5f, true, 5);

        column.add(testEntity2);
        column.add(testEntity1);
        column.add(testEntity3);
        column.add(testEntity4);
        column.add(testEntity5);
        column.complete();

        ADBColumnSorted sortedColumn = column.getSortedColumn(new ADBEntityDoubleEntry(0, 1), new ADBEntityDoubleEntry(2, 3));
        assertThat(sortedColumn instanceof ADBDoubleColumnSorted).isTrue();
        assertThat(sortedColumn.size()).isEqualTo(3);

        ObjectList<ADBEntityEntry> sortedEntries = sortedColumn.materializeSorted();

        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(0).getId())).isEqualTo(1);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(1).getId())).isEqualTo(0);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(2).getId())).isEqualTo(2);

        assertThat(sortedEntries.get(0).getValueField().getDouble(sortedEntries.get(0))).isEqualTo(1);
        assertThat(sortedEntries.get(1).getValueField().getDouble(sortedEntries.get(1))).isEqualTo(2);
        assertThat(sortedEntries.get(2).getValueField().getDouble(sortedEntries.get(2))).isEqualTo(3);
    }

    @Test
    public void shouldReturnCorrectSortedColumnLimitedMin() throws NoSuchFieldException, IllegalAccessException {
        Field field = TestEntity.class.getDeclaredField("aInteger");
        ADBDoubleColumn column = new ADBDoubleColumn(field, PARTITION_ID);

        TestEntity testEntity1 = new TestEntity(1, "Test1", 1f, true, 1);
        TestEntity testEntity2 = new TestEntity(2, "Test2", 2f, true, 2);
        TestEntity testEntity3 = new TestEntity(3, "Test3", 3f, true, 3);
        TestEntity testEntity4 = new TestEntity(4, "Test4", 4f, true, 4);
        TestEntity testEntity5 = new TestEntity(5, "Test5", 5f, true, 5);

        column.add(testEntity2);
        column.add(testEntity1);
        column.add(testEntity3);
        column.add(testEntity5);
        column.add(testEntity4);
        column.complete();

        ADBColumnSorted sortedColumn = column.getSortedColumn(new ADBEntityDoubleEntry(2, 3), new ADBEntityDoubleEntry(4, 5));
        assertThat(sortedColumn instanceof ADBDoubleColumnSorted).isTrue();
        assertThat(sortedColumn.size()).isEqualTo(3);

        ObjectList<ADBEntityEntry> sortedEntries = sortedColumn.materializeSorted();

        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(0).getId())).isEqualTo(2);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(1).getId())).isEqualTo(4);
        assertThat(ADBInternalIDHelper.getEntityId(sortedEntries.get(2).getId())).isEqualTo(3);

        assertThat(sortedEntries.get(0).getValueField().getDouble(sortedEntries.get(0))).isEqualTo(3);
        assertThat(sortedEntries.get(1).getValueField().getDouble(sortedEntries.get(1))).isEqualTo(4);
        assertThat(sortedEntries.get(2).getValueField().getDouble(sortedEntries.get(2))).isEqualTo(5);
    }

}