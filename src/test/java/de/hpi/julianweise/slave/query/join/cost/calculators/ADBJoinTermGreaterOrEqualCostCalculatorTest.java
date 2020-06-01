package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntryFactory;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBIntervalImpl;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.SneakyThrows;
import org.junit.Test;

import java.lang.reflect.Field;

import static de.hpi.julianweise.slave.query.join.cost.interval.ADBIntervalImpl.NO_INTERSECTION;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBJoinTermGreaterOrEqualCostCalculatorTest {

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
        ADBJoinTermGreaterOrEqualCostCalculator calculator = new ADBJoinTermGreaterOrEqualCostCalculator();

        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isZero();
    }

    @Test
    public void expectEmptyArrayForEmptyLeftLists() {
        ADBJoinTermGreaterOrEqualCostCalculator calculator = new ADBJoinTermGreaterOrEqualCostCalculator();

        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(1, 1));
        right.add(this.createTestEntry(2, 2));
        right.add(this.createTestEntry(3, 3));

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isZero();
    }

    @Test
    public void expectMoIntersectsForEmptyRightLists() {
        ADBJoinTermGreaterOrEqualCostCalculator calculator = new ADBJoinTermGreaterOrEqualCostCalculator();

        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(1, 1));
        left.add(this.createTestEntry(2, 2));
        left.add(this.createTestEntry(3, 3));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isEqualTo(3);
        assertThat(result[0]).isEqualTo(NO_INTERSECTION);
        assertThat(result[1]).isEqualTo(NO_INTERSECTION);
        assertThat(result[2]).isEqualTo(NO_INTERSECTION);
    }

    @Test
    public void expectValidResultsForBothListsFilled() {
        ADBJoinTermGreaterOrEqualCostCalculator calculator = new ADBJoinTermGreaterOrEqualCostCalculator();

        ObjectList<ADBEntityEntry> left = new ObjectArrayList<>();
        left.add(this.createTestEntry(0, 0));
        left.add(this.createTestEntry(1, 1));
        left.add(this.createTestEntry(2, 2));
        left.add(this.createTestEntry(3, 3));
        ObjectList<ADBEntityEntry> right = new ObjectArrayList<>();
        right.add(this.createTestEntry(1, 1));
        right.add(this.createTestEntry(2, 2));
        right.add(this.createTestEntry(2, 3));
        right.add(this.createTestEntry(3, 4));

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isEqualTo(4);
        assertThat(result[0]).isEqualTo(NO_INTERSECTION);
        assertThat(result[1]).isEqualTo(new ADBIntervalImpl(0, 0));
        assertThat(result[2]).isEqualTo(new ADBIntervalImpl(0, 2));
        assertThat(result[3]).isEqualTo(new ADBIntervalImpl(0, 3));
    }

}