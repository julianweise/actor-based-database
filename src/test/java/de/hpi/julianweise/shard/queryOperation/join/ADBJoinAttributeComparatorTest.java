package de.hpi.julianweise.shard.queryOperation.join;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.utility.largeMessageTransfer.ADBPair;
import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.csv.TestEntityFactory;
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
        new ADBEntityFactoryProvider(new TestEntityFactory());
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
        TestProbe<ADBLocalCompareAttributesSession.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger");
        List<ADBEntityType> localEntities = Collections.singletonList(new TestEntity(1, "Test", 1f, true, 1.01, 'a'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare.builder()
                                                                                       .startIndexSourceAttributeValues(0)
                                                                                       .endIndexSourceAttributeValues(1)
                                                                                       .term(term)
                                                                                       .respondTo(respondTo.ref())
                                                                                       .sourceAttributeValues(Collections.singletonList(new ADBPair<>(1, 2)))
                                                                                       .targetAttributeValues(Collections.singletonMap("aInteger", ADBSortedEntityAttributes
                                                                                               .of("aInteger", localEntities)))
                                                                                       .build();

        comparator.tell(message);

        ADBLocalCompareAttributesSession.HandleResults results =
                respondTo.expectMessageClass(ADBLocalCompareAttributesSession.HandleResults.class);

        assertThat(results.getTerm()).isEqualTo(term);
        assertThat(results.getJoinPartners().size()).isOne();
        // index for data stored on source
        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(2);
        // index for data stored locally
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(0);
        assertThat(((TestEntity) localEntities.get(results.getJoinPartners().get(0).getValue())).aInteger).isEqualTo(1);
    }

    @Test
    public void testCompareOneTupleWithNoExpectedResultEquality() {
        TestProbe<ADBLocalCompareAttributesSession.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger");
        List<ADBEntityType> targetEntities = Collections.singletonList(new TestEntity(1, "Test", 1f, true, 1.01, 'a'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .startIndexSourceAttributeValues(0)
                .endIndexSourceAttributeValues(1)
                .term(term)
                .respondTo(respondTo.ref())
                .sourceAttributeValues(Collections.singletonList(new ADBPair<>(2, 2)))
                .targetAttributeValues(Collections.singletonMap("aInteger", ADBSortedEntityAttributes
                        .of("aInteger", targetEntities)))
                .build();

        comparator.tell(message);

        ADBLocalCompareAttributesSession.HandleResults results =
                respondTo.expectMessageClass(ADBLocalCompareAttributesSession.HandleResults.class);

        assertThat(results.getTerm()).isEqualTo(term);
        assertThat(results.getJoinPartners().size()).isZero();
    }

    @Test
    public void testCompareMultipleTargetTuplesWithExpectedResultEquality() {
        TestProbe<ADBLocalCompareAttributesSession.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "bString", "bString");
        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(1, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(2, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(3, "Test", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .startIndexSourceAttributeValues(0)
                .endIndexSourceAttributeValues(1)
                .term(term)
                .respondTo(respondTo.ref())
                .sourceAttributeValues(Collections.singletonList(new ADBPair<>("Test", 2)))
                .targetAttributeValues(Collections.singletonMap("bString", ADBSortedEntityAttributes
                        .of("bString", targetEntities)))
                .build();

        comparator.tell(message);

        ADBLocalCompareAttributesSession.HandleResults results =
                respondTo.expectMessageClass(ADBLocalCompareAttributesSession.HandleResults.class);

        assertThat(results.getTerm()).isEqualTo(term);
        assertThat(results.getJoinPartners().size()).isEqualTo(3);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(2);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(2);
    }

    @Test
    public void testCompareMultipleSourceAndTargetTuplesWithExpectedResultEquality() {
        TestProbe<ADBLocalCompareAttributesSession.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "bString", "bString");

        List<ADBPair<Comparable<?>, Integer>> sourceEntities = new ArrayList<>();
        sourceEntities.add(new ADBPair<>("Test", 4));
        sourceEntities.add(new ADBPair<>("Test2", 5));
        sourceEntities.add(new ADBPair<>("Test", 6));

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(5, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(6, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(7, "Test3", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .startIndexSourceAttributeValues(0)
                .endIndexSourceAttributeValues(3)
                .term(term)
                .respondTo(respondTo.ref())
                .sourceAttributeValues(sourceEntities)
                .targetAttributeValues(Collections.singletonMap("bString", ADBSortedEntityAttributes
                        .of("bString", targetEntities)))
                .build();

        comparator.tell(message);

        ADBLocalCompareAttributesSession.HandleResults results =
                respondTo.expectMessageClass(ADBLocalCompareAttributesSession.HandleResults.class);

        assertThat(results.getTerm()).isEqualTo(term);
        assertThat(results.getJoinPartners().size()).isEqualTo(4);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(4);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(4);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(1);

        assertThat(results.getJoinPartners().get(2).getKey()).isEqualTo(6);
        assertThat(results.getJoinPartners().get(2).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(3).getKey()).isEqualTo(6);
        assertThat(results.getJoinPartners().get(3).getValue()).isEqualTo(1);
    }

    @Test
    public void testCompareMultipleSourceAndTargetTuplesWithExpectedResultEqualityCorrectlyChunked() {
        TestProbe<ADBLocalCompareAttributesSession.Command> respondTo = testKit.createTestProbe();

        Behavior<ADBJoinAttributeComparator.Command> behavior = ADBJoinAttributeComparatorFactory.createDefault();
        ActorRef<ADBJoinAttributeComparator.Command> comparator = testKit.spawn(behavior);

        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "bString", "bString");

        List<ADBPair<Comparable<?>, Integer>> sourceEntities = new ArrayList<>();
        sourceEntities.add(new ADBPair<>("Test", 4));
        sourceEntities.add(new ADBPair<>("Test2", 5));
        sourceEntities.add(new ADBPair<>("Test", 6));

        List<ADBEntityType> targetEntities = new ArrayList<>();
        targetEntities.add(new TestEntity(5, "Test", 1f, true, 1.01, 'a'));
        targetEntities.add(new TestEntity(6, "Test", 2f, true, 2.01, 'b'));
        targetEntities.add(new TestEntity(7, "Test3", 3f, true, 3.01, 'c'));

        ADBJoinAttributeComparator.Compare message = ADBJoinAttributeComparator.Compare
                .builder()
                .startIndexSourceAttributeValues(0)
                .endIndexSourceAttributeValues(2)
                .term(term)
                .respondTo(respondTo.ref())
                .sourceAttributeValues(sourceEntities)
                .targetAttributeValues(Collections.singletonMap("bString", ADBSortedEntityAttributes
                        .of("bString", targetEntities)))
                .build();

        comparator.tell(message);

        ADBLocalCompareAttributesSession.HandleResults results =
                respondTo.expectMessageClass(ADBLocalCompareAttributesSession.HandleResults.class);

        assertThat(results.getTerm()).isEqualTo(term);
        assertThat(results.getJoinPartners().size()).isEqualTo(2);

        assertThat(results.getJoinPartners().get(0).getKey()).isEqualTo(4);
        assertThat(results.getJoinPartners().get(0).getValue()).isEqualTo(0);

        assertThat(results.getJoinPartners().get(1).getKey()).isEqualTo(4);
        assertThat(results.getJoinPartners().get(1).getValue()).isEqualTo(1);
    }

}