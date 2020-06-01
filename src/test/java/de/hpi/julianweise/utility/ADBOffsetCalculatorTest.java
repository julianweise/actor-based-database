package de.hpi.julianweise.utility;


import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.csv.TestEntityFactory;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntryFactory;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import de.hpi.julianweise.utility.query.join.ADBOffsetCalculator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBOffsetCalculatorTest {

    @Before
    public void setUp() {
        ADBEntityFactoryProvider.initialize(new TestEntityFactory());
        ADBComparator.buildComparatorMapping();
    }

    private ADBEntity createTestEntity(int intValue) {
        ADBEntity entity = new TestEntity(intValue, "a", 1f, true, 1.1);
        entity.setInternalID(ADBInternalIDHelper.createID(1, 1, 1));
        return entity;
    }

    @SneakyThrows
    private ADBEntityEntry createTestEntry(int intValue) {
        Field field = TestEntity.class.getDeclaredField("aInteger");
        return ADBEntityEntryFactory.of(this.createTestEntity(intValue), field);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetWithDirectMatches() {
        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(5));
        left.add(this.createTestEntry(8));
        left.add(this.createTestEntry(13));
        left.add(this.createTestEntry(22));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(1));
        right.add(this.createTestEntry(13));
        right.add(this.createTestEntry(16));
        right.add(this.createTestEntry(22));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
        assertThat(leftToRightOffset[1]).isEqualTo(1);
        assertThat(leftToRightOffset[2]).isEqualTo(1);
        assertThat(leftToRightOffset[3]).isEqualTo(3);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetsWithoutDirectMatches() {
        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(5));
        left.add(this.createTestEntry(8));
        left.add(this.createTestEntry(13));
        left.add(this.createTestEntry(22));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(1));
        right.add(this.createTestEntry(14));
        right.add(this.createTestEntry(16));
        right.add(this.createTestEntry(17));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
        assertThat(leftToRightOffset[1]).isEqualTo(1);
        assertThat(leftToRightOffset[2]).isEqualTo(1);
        assertThat(leftToRightOffset[3]).isEqualTo(3);
    }

    @Test
    public void expectCorrectResultSkewedSetsRight() {
        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(5));
        left.add(this.createTestEntry(8));
        left.add(this.createTestEntry(13));
        left.add(this.createTestEntry(22));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(17));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(0);
        assertThat(leftToRightOffset[1]).isEqualTo(0);
        assertThat(leftToRightOffset[2]).isEqualTo(0);
        assertThat(leftToRightOffset[3]).isEqualTo(0);
    }

    @Test
    public void expectCorrectResultSkewedSetsLeft() {
        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(13));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(1));
        right.add(this.createTestEntry(14));
        right.add(this.createTestEntry(16));
        right.add(this.createTestEntry(17));


        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(1);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
    }

    @Test
    public void expectCorrectResultLeftEmpty() {
        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(1));
        right.add(this.createTestEntry(14));
        right.add(this.createTestEntry(16));
        right.add(this.createTestEntry(17));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(0);
    }

    @Test
    public void expectCorrectResultRightEmpty() {
        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(5));
        left.add(this.createTestEntry(8));
        left.add(this.createTestEntry(13));
        left.add(this.createTestEntry(22));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(-1);
        assertThat(leftToRightOffset[1]).isEqualTo(-1);
        assertThat(leftToRightOffset[2]).isEqualTo(-1);
        assertThat(leftToRightOffset[3]).isEqualTo(-1);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetWithDuplicatesRight() {
        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(5));
        left.add(this.createTestEntry(8));
        left.add(this.createTestEntry(13));
        left.add(this.createTestEntry(22));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(1));
        right.add(this.createTestEntry(13));
        right.add(this.createTestEntry(13));
        right.add(this.createTestEntry(16));
        right.add(this.createTestEntry(22));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
        assertThat(leftToRightOffset[1]).isEqualTo(1);
        assertThat(leftToRightOffset[2]).isEqualTo(2);
        assertThat(leftToRightOffset[3]).isEqualTo(4);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetWithDuplicatesLeft() {
        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(8));
        left.add(this.createTestEntry(8));
        left.add(this.createTestEntry(12));
        left.add(this.createTestEntry(12));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(1));
        right.add(this.createTestEntry(8));
        right.add(this.createTestEntry(13));
        right.add(this.createTestEntry(16));
        right.add(this.createTestEntry(22));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
        assertThat(leftToRightOffset[1]).isEqualTo(1);
        assertThat(leftToRightOffset[2]).isEqualTo(2);
        assertThat(leftToRightOffset[3]).isEqualTo(2);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetWithDuplicatesBoth() {
        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(8));
        left.add(this.createTestEntry(8));
        left.add(this.createTestEntry(12));
        left.add(this.createTestEntry(12));
        left.add(this.createTestEntry(13));
        left.add(this.createTestEntry(13));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(1));
        right.add(this.createTestEntry(8));
        right.add(this.createTestEntry(8));
        right.add(this.createTestEntry(13));
        right.add(this.createTestEntry(13));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(6);
        assertThat(leftToRightOffset[0]).isEqualTo(2);
        assertThat(leftToRightOffset[1]).isEqualTo(2);
        assertThat(leftToRightOffset[2]).isEqualTo(3);
        assertThat(leftToRightOffset[3]).isEqualTo(3);
        assertThat(leftToRightOffset[4]).isEqualTo(4);
        assertThat(leftToRightOffset[5]).isEqualTo(4);
    }


    @Test
    public void expectCorrectResultsForManyItemsLarger() {
        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(5));
        left.add(this.createTestEntry(8));
        left.add(this.createTestEntry(13));
        left.add(this.createTestEntry(22));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(1));
        right.add(this.createTestEntry(2));
        right.add(this.createTestEntry(3));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(2);
        assertThat(leftToRightOffset[1]).isEqualTo(2);
        assertThat(leftToRightOffset[2]).isEqualTo(2);
        assertThat(leftToRightOffset[3]).isEqualTo(2);
    }

    @Test
    public void expectCorrectResultsForManyItemsLargerSmaller() {
        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(5));
        left.add(this.createTestEntry(8));
        left.add(this.createTestEntry(13));
        left.add(this.createTestEntry(22));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(22));
        right.add(this.createTestEntry(24));
        right.add(this.createTestEntry(33));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(0);
        assertThat(leftToRightOffset[1]).isEqualTo(0);
        assertThat(leftToRightOffset[2]).isEqualTo(0);
        assertThat(leftToRightOffset[3]).isEqualTo(0);
    }

}