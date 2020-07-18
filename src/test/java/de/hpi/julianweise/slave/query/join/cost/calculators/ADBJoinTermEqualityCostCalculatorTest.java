package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.csv.TestEntityFactory;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntryFactory;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityIntEntry;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBJoinTermEqualityCostCalculatorTest {

    @Before
    public void setUp() {
        ADBEntityFactoryProvider.initialize(new TestEntityFactory());
        ADBComparator.buildComparatorMapping();
    }

    private ADBEntity createTestEntity(int id, int intValue) {
        ADBEntity entity = new TestEntity(intValue, "a", 1f, true, 1.1);
        entity.setInternalID(ADBInternalIDHelper.createID(1, 1, id));
        return entity;
    }

    @SneakyThrows
    private ADBEntityEntry createTestEntry(int intValue, int id) {
        Field field = TestEntity.class.getDeclaredField("aInteger");
        return ADBEntityEntryFactory.of(this.createTestEntity(id, intValue), field);
    }
    
    @Test
    public void expectEmptyArrayForEmptyLists() {
        ADBJoinTermEqualityCostCalculator calculator = new ADBJoinTermEqualityCostCalculator();

        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();

        ADBComparator comparator = ADBComparator.getFor(ADBEntityIntEntry.valueField, ADBEntityIntEntry.valueField);
        ADBInterval[][] result = calculator.calc(left, right, comparator);
        assertThat(result.length).isZero();
    }

    @Test
    public void expectEmptyArrayForEmptyLeftLists() {
        ADBJoinTermEqualityCostCalculator calculator = new ADBJoinTermEqualityCostCalculator();

        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(1, 1));
        right.add(this.createTestEntry(2, 2));
        right.add(this.createTestEntry(3, 3));

        ADBComparator comparator = ADBComparator.getFor(ADBEntityIntEntry.valueField, ADBEntityIntEntry.valueField);
        ADBInterval[][] result = calculator.calc(left, right, comparator);
        assertThat(result.length).isZero();
    }

    @Test
    public void expectMoIntersectsForEmptyRightLists() {
        ADBJoinTermEqualityCostCalculator calculator = new ADBJoinTermEqualityCostCalculator();

        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(1, 1));
        left.add(this.createTestEntry(2, 2));
        left.add(this.createTestEntry(3, 3));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();

        ADBComparator comparator = ADBComparator.getFor(ADBEntityIntEntry.valueField, ADBEntityIntEntry.valueField);
        ADBInterval[][] result = calculator.calc(left, right, comparator);
        assertThat(result.length).isZero();
    }

    @Test
    public void expectValidResultsForBothListsFilled() {
        ADBJoinTermEqualityCostCalculator calculator = new ADBJoinTermEqualityCostCalculator();

        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(1, 1));
        left.add(this.createTestEntry(2, 2));
        left.add(this.createTestEntry(3, 3));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(1, 1));
        right.add(this.createTestEntry(2, 2));
        right.add(this.createTestEntry(2, 3));

        ADBComparator comparator = ADBComparator.getFor(ADBEntityIntEntry.valueField, ADBEntityIntEntry.valueField);
        ADBInterval[][] result = calculator.calc(left, right, comparator);
        assertThat(result.length).isEqualTo(2);
        assertThat(result[0][0]).isEqualTo(new ADBInterval(0, 0));
        assertThat(result[1][0]).isEqualTo(new ADBInterval(1, 2));
    }

    @Test
    public void expectValidResultsForBothListsFilledWithDuplicates() {
        ADBJoinTermEqualityCostCalculator calculator = new ADBJoinTermEqualityCostCalculator();

        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(1, 1));
        left.add(this.createTestEntry(2, 2));
        left.add(this.createTestEntry(2, 3));
        left.add(this.createTestEntry(3, 4));
        left.add(this.createTestEntry(3, 5));
        left.add(this.createTestEntry(4, 6));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(1, 1));
        right.add(this.createTestEntry(2, 2));
        right.add(this.createTestEntry(2, 3));
        right.add(this.createTestEntry(3, 4));
        right.add(this.createTestEntry(4, 5));

        ADBComparator comparator = ADBComparator.getFor(ADBEntityIntEntry.valueField, ADBEntityIntEntry.valueField);
        ADBInterval[][] result = calculator.calc(left, right, comparator);
        assertThat(result.length).isEqualTo(6);
        assertThat(result[0][0]).isEqualTo(new ADBInterval(0, 0));
        assertThat(result[1][0]).isEqualTo(new ADBInterval(1, 2));
        assertThat(result[2][0]).isEqualTo(new ADBInterval(1, 2));
        assertThat(result[3][0]).isEqualTo(new ADBInterval(3, 3));
        assertThat(result[4][0]).isEqualTo(new ADBInterval(3, 3));
        assertThat(result[5][0]).isEqualTo(new ADBInterval(4, 4));
    }

}