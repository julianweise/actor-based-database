package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.csv.TestEntityFactory;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBJoinAttributeComparatorTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @Before
    public void prepare() {
        ADBEntityFactoryProvider.initialize(new TestEntityFactory());
    }

    @After
    public void cleanup() {
        testKit.after();
        testKit = new TestKitJunitResource();
    }

    @AfterClass
    public static void after() {
        testKit.after();
    }

    @Test
    public void testCompareOneTupleWithExpectedResultEquality() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBEntityType> localEntities = Collections
                .singletonList(new TestEntity(1, "Test", 1f, true, 1.01, 'a'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.EQUALITY)
                .respondTo(respondTo.ref())
                .leftSideValues(Collections.singletonList(new ADBPair<>(1, 2)))
                .rightSideValues(ADBSortedEntityAttributes.of("aInteger", localEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isOne();
        // index for data stored on source
        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(2);
        // index for data stored locally
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(0);
        assertThat(((TestEntity) localEntities.get(results.getJoinPartners().get(0).getValue())).getAInteger()).isEqualTo(1);
    }

    @Test
    public void testCompareOneTupleWithNoExpectedResultEquality() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBEntityType> targetEntities = Collections.singletonList(new TestEntity(1, "Test", 1f, true, 1.01, 'a'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.EQUALITY)
                .respondTo(respondTo.ref())
                .leftSideValues(Collections.singletonList(new ADBPair<>(2, 2)))
                .rightSideValues(ADBSortedEntityAttributes
                        .of("aInteger", targetEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isZero();
    }

    @Test
    public void testCompareMultipleTargetTuplesWithExpectedResultEquality() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(1, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(2, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(3, "Test", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.EQUALITY)
                .respondTo(respondTo.ref())
                .leftSideValues(Collections.singletonList(new ADBPair<>("Test", 2)))
                .rightSideValues(ADBSortedEntityAttributes.of("bString", targetEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isEqualTo(3);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(0);
    }

    @Test
    public void testCompareMultipleSourceAndTargetTuplesWithExpectedResultEquality() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBPair<Comparable<?>, Integer>> sourceEntities = new ArrayList<>();
        sourceEntities.add(new ADBPair<>("Test", 0));
        sourceEntities.add(new ADBPair<>("Test", 1));
        sourceEntities.add(new ADBPair<>("Test2", 2));

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(5, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(6, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(7, "Test3", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.EQUALITY)
                .respondTo(respondTo.ref())
                .leftSideValues(sourceEntities)
                .rightSideValues(ADBSortedEntityAttributes.of("bString", targetEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isEqualTo(4);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(3).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(3).getValue()).isEqualTo(0);
    }

    @Test
    public void testCompareMultipleSourceAndTargetTuplesWithExpectedResultNumericEquality() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBPair<Comparable<?>, Integer>> sourceEntities = new ArrayList<>();
        sourceEntities.add(new ADBPair<>(5, 0));
        sourceEntities.add(new ADBPair<>(8, 1));
        sourceEntities.add(new ADBPair<>(9, 2));

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(5, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(5, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(9, "Test3", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.EQUALITY)
                .respondTo(respondTo.ref())
                .leftSideValues(sourceEntities)
                .rightSideValues(ADBSortedEntityAttributes.of("aInteger", targetEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isEqualTo(3);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(2);

    }

    @Test
    public void testCompareMultipleSourceAndTargetTuplesWithExpectedResultLessNumeric() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBPair<Comparable<?>, Integer>> sourceEntities = new ArrayList<>();
        sourceEntities.add(new ADBPair<>(4, 0));
        sourceEntities.add(new ADBPair<>(5, 1));
        sourceEntities.add(new ADBPair<>(6, 2));

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(5, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(6, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(7, "Test3", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.LESS)
                .respondTo(respondTo.ref())
                .leftSideValues(sourceEntities)
                .rightSideValues(ADBSortedEntityAttributes.of("aInteger", targetEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isEqualTo(6);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(3).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(3).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(4).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(4).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(5).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(5).getValue()).isEqualTo(2);
    }

    @Test
    public void testCompareMultipleSourceAndTargetTuplesWithExpectedResultLessNumericIncludingDuplicatesLeft() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBPair<Comparable<?>, Integer>> sourceEntities = new ArrayList<>();
        sourceEntities.add(new ADBPair<>(4, 0));
        sourceEntities.add(new ADBPair<>(5, 1));
        sourceEntities.add(new ADBPair<>(5, 2));
        sourceEntities.add(new ADBPair<>(6, 3));

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(4, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(6, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(7, "Test3", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.LESS)
                .respondTo(respondTo.ref())
                .leftSideValues(sourceEntities)
                .rightSideValues(ADBSortedEntityAttributes.of("aInteger", targetEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isEqualTo(7);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(3).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(3).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(4).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(4).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(5).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(5).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(6).getKey()).isEqualTo(3);
        assertThat(results.getJoinPartners().get(6).getValue()).isEqualTo(2);
    }

    @Test
    public void testCompareMultipleSourceAndTargetTuplesWithExpectedResultLessNumericDuplicatesRight() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBPair<Comparable<?>, Integer>> sourceEntities = new ArrayList<>();
        sourceEntities.add(new ADBPair<>(4, 0));
        sourceEntities.add(new ADBPair<>(5, 1));
        sourceEntities.add(new ADBPair<>(6, 2));
        sourceEntities.add(new ADBPair<>(7, 2));

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(5, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(6, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(6, "Test2", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(7, "Test3", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.LESS)
                .respondTo(respondTo.ref())
                .leftSideValues(sourceEntities)
                .rightSideValues(ADBSortedEntityAttributes.of("aInteger", targetEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isEqualTo(8);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(3).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(3).getValue()).isEqualTo(3);

        assertThat(results.getJoinPartners().get(4).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(4).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(5).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(5).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(6).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(6).getValue()).isEqualTo(3);

        assertThat(results.getJoinPartners().get(7).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(7).getValue()).isEqualTo(3);
    }

    @Test
    public void testCompareMultipleSourceAndTargetTuplesWithExpectedResultLessOrEqualNumeric() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBPair<Comparable<?>, Integer>> sourceEntities = new ArrayList<>();
        sourceEntities.add(new ADBPair<>(4, 0));
        sourceEntities.add(new ADBPair<>(5, 1));
        sourceEntities.add(new ADBPair<>(6, 2));

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(5, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(6, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(7, "Test3", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL)
                .respondTo(respondTo.ref())
                .leftSideValues(sourceEntities)
                .rightSideValues(ADBSortedEntityAttributes.of("aInteger", targetEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isEqualTo(8);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(3).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(3).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(4).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(4).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(5).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(5).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(6).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(6).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(7).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(7).getValue()).isEqualTo(2);
    }

    @Test
    public void testCompareMultipleSourceAndTargetTuplesWithExpectedResultGreaterNumeric() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBPair<Comparable<?>, Integer>> sourceEntities = new ArrayList<>();
        sourceEntities.add(new ADBPair<>(4, 0));
        sourceEntities.add(new ADBPair<>(6, 1));
        sourceEntities.add(new ADBPair<>(8, 2));

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(5, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(6, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(7, "Test3", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.GREATER)
                .respondTo(respondTo.ref())
                .leftSideValues(sourceEntities)
                .rightSideValues(ADBSortedEntityAttributes.of("aInteger", targetEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isEqualTo(4);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(3).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(3).getValue()).isEqualTo(0);
    }

    @Test
    public void testCompareMultipleSourceAndTargetTuplesWithExpectedResultGreaterOrEqualNumeric() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBPair<Comparable<?>, Integer>> sourceEntities = new ArrayList<>();
        sourceEntities.add(new ADBPair<>(4, 0));
        sourceEntities.add(new ADBPair<>(6, 1));
        sourceEntities.add(new ADBPair<>(8, 2));

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(5, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(6, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(7, "Test3", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL)
                .respondTo(respondTo.ref())
                .leftSideValues(sourceEntities)
                .rightSideValues(ADBSortedEntityAttributes.of("aInteger", targetEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isEqualTo(5);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(3).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(3).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(4).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(4).getValue()).isEqualTo(0);
    }

    @Test
    public void testCompareMultipleSourceAndTargetTuplesWithExpectedResultGreaterOrEqualNumericIncludingDuplicatesLeft() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBPair<Comparable<?>, Integer>> sourceEntities = new ArrayList<>();
        sourceEntities.add(new ADBPair<>(4, 0));
        sourceEntities.add(new ADBPair<>(6, 1));
        sourceEntities.add(new ADBPair<>(6, 2));
        sourceEntities.add(new ADBPair<>(9, 3));

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(5, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(6, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(7, "Test3", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL)
                .respondTo(respondTo.ref())
                .leftSideValues(sourceEntities)
                .rightSideValues(ADBSortedEntityAttributes.of("aInteger", targetEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isEqualTo(7);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(3).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(3).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(4).getKey()).isEqualTo(3);
        assertThat(results.getJoinPartners().get(4).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(5).getKey()).isEqualTo(3);
        assertThat(results.getJoinPartners().get(5).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(6).getKey()).isEqualTo(3);
        assertThat(results.getJoinPartners().get(6).getValue()).isEqualTo(0);
    }

    @Test
    public void testCompareMultipleSourceAndTargetTuplesWithExpectedResultGreaterOrEqualNumericIncludingDuplicatesRight() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBPair<Comparable<?>, Integer>> sourceEntities = new ArrayList<>();
        sourceEntities.add(new ADBPair<>(4, 0));
        sourceEntities.add(new ADBPair<>(6, 1));
        sourceEntities.add(new ADBPair<>(9, 2));

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(5, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(6, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(6, "Test2", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(7, "Test3", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL)
                .respondTo(respondTo.ref())
                .leftSideValues(sourceEntities)
                .rightSideValues(ADBSortedEntityAttributes.of("aInteger", targetEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isEqualTo(7);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(3).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(3).getValue()).isEqualTo(3);

        assertThat(results.getJoinPartners().get(4).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(4).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(5).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(5).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(6).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(6).getValue()).isEqualTo(0);
    }


    @Test
    public void testCompareMultipleSourceAndTargetTuplesWithExpectedResultLessFloat() {
        TestProbe<ADBJoinTermComparator.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        List<ADBPair<Comparable<?>, Integer>> sourceEntities = new ArrayList<>();
        sourceEntities.add(new ADBPair<>(1.01f, 0));
        sourceEntities.add(new ADBPair<>(1.011f, 1));
        sourceEntities.add(new ADBPair<>(1.02f, 2));

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(5, "Test", 1.001f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(6, "Test", 1.0111f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(7, "Test3", 1.03f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .operator(ADBQueryTerm.RelationalOperator.LESS)
                .respondTo(respondTo.ref())
                .leftSideValues(sourceEntities)
                .rightSideValues(ADBSortedEntityAttributes.of("cFloat", targetEntities).getAllWithOriginalIndex())
                .build();

        comparator.tell(message);

        ADBJoinTermComparator.CompareAttributesChunkResult results =
                respondTo.expectMessageClass(ADBJoinTermComparator.CompareAttributesChunkResult.class);

        assertThat(results.getJoinPartners().size()).isEqualTo(5);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(0);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(3).getKey()).isEqualTo(1);
        assertThat(results.getJoinPartners().get(3).getValue()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(4).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(4).getValue()).isEqualTo(2);
    }
}