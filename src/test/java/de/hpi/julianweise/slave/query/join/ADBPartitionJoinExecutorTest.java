package de.hpi.julianweise.slave.query.join;


import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.csv.TestEntityFactory;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntryFactory;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.join.node.ADBPartitionJoinTask;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.SneakyThrows;
import org.agrona.collections.Object2ObjectHashMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBPartitionJoinExecutorTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @Before
    public void setUp() {
        ADBEntityFactoryProvider.initialize(new TestEntityFactory());
        ADBComparator.buildComparatorMapping();
        testKit.spawn(ADBSlave.create());
    }

    @After
    public void cleanup() {
        testKit.after();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
        testKit = new TestKitJunitResource();
    }

    @AfterClass
    public static void after() {
        testKit.after();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
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
    public void expectEqualityJoinToBeExecutedSuccessfully() {
        int lPartitionId = 0;
        int rPartitionId = 0;

        TestProbe<ADBPartitionJoinExecutor.Response> supervisor = testKit.createTestProbe();

        TestProbe<ADBPartitionManager.Command> leftPartitionManager = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> rightPartitionManager = testKit.createTestProbe();

        Map<String, ObjectList<ADBEntityEntry>> leftSideAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBEntityEntry> fIntegerAttributes = new ObjectArrayList<>();
        fIntegerAttributes.add(this.createTestEntry(1, 0));
        fIntegerAttributes.add(this.createTestEntry(3, 1));
        fIntegerAttributes.add(this.createTestEntry(5, 2));
        leftSideAttributes.put("aInteger", fIntegerAttributes);

        Map<String, ObjectList<ADBEntityEntry>> rightSideAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBEntityEntry> lIntegerAttributes = new ObjectArrayList<>();
        lIntegerAttributes.add(this.createTestEntry(3, 0));
        lIntegerAttributes.add(this.createTestEntry(5, 1));
        lIntegerAttributes.add(this.createTestEntry(9, 2));
        rightSideAttributes.put("aInteger", lIntegerAttributes);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        Behavior<ADBPartitionJoinExecutor.Command> behavior = ADBPartitionJoinExecutorFactory
                .createDefault(joinQuery, supervisor.ref());
        ActorRef<ADBPartitionJoinExecutor.Command> executor = testKit.spawn(behavior);

        ADBPartitionJoinTask task = ADBPartitionJoinTask.builder()
                                                        .leftPartitionManager(leftPartitionManager.ref())
                                                        .leftPartitionId(lPartitionId)
                                                        .rightPartitionManager(rightPartitionManager.ref())
                                                        .rightPartitionId(rPartitionId)
                                                        .build();

        executor.tell(new ADBPartitionJoinExecutor.Prepare(task));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesLeft =
                leftPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(requestJoinAttributesLeft.getMessage().getAttributes()).containsAll(joinQuery.getAllLeftHandSideFields());

        executor.tell(new ADBPartition.MultipleAttributes(leftSideAttributes));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesRight =
                rightPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(requestJoinAttributesRight.getMessage().getAttributes()).containsAll(joinQuery.getAllRightHandSideFields());

        executor.tell(new ADBPartition.MultipleAttributes(rightSideAttributes));

        executor.tell(new ADBPartitionJoinExecutor.Execute());

        ADBPartitionJoinExecutor.JoinTaskPrepared responsePrepared =
                supervisor.expectMessageClass(ADBPartitionJoinExecutor.JoinTaskPrepared.class);

        assertThat(responsePrepared.getRespondTo()).isEqualTo(executor);

        executor.tell(new ADBPartitionJoinExecutor.Execute());

        ADBPartitionJoinExecutor.PartitionsJoined responseResults =
                supervisor.expectMessageClass(ADBPartitionJoinExecutor.PartitionsJoined.class);

        assertThat(responseResults.getJoinTuples().size()).isEqualTo(2);
        assertThat(ADBInternalIDHelper.getEntityId(responseResults.getJoinTuples().get(0).getKey())).isEqualTo(1);
        assertThat(ADBInternalIDHelper.getEntityId(responseResults.getJoinTuples().get(0).getValue())).isEqualTo(0);
        assertThat(ADBInternalIDHelper.getEntityId(responseResults.getJoinTuples().get(1).getKey())).isEqualTo(2);
        assertThat(ADBInternalIDHelper.getEntityId(responseResults.getJoinTuples().get(1).getValue())).isEqualTo(1);
    }

    @Test
    public void expectLessReversedJoinToBeExecutedSuccessfully() {
        int lPartitionId = 0;
        int rPartitionId = 0;

        TestProbe<ADBPartitionJoinExecutor.Response> supervisor = testKit.createTestProbe();

        TestProbe<ADBPartitionManager.Command> leftPartitionManager = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> rightPartitionManager = testKit.createTestProbe();

        Map<String, ObjectList<ADBEntityEntry>> rightSideAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBEntityEntry> fIntegerAttributes = new ObjectArrayList<>();
        fIntegerAttributes.add(this.createTestEntry(1, 0));
        fIntegerAttributes.add(this.createTestEntry(3, 1));
        fIntegerAttributes.add(this.createTestEntry(5, 2));
        rightSideAttributes.put("aInteger", fIntegerAttributes);

        Map<String, ObjectList<ADBEntityEntry>> leftSideAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBEntityEntry> lIntegerAttributes = new ObjectArrayList<>();
        lIntegerAttributes.add(this.createTestEntry(3, 0));
        lIntegerAttributes.add(this.createTestEntry(5, 1));
        lIntegerAttributes.add(this.createTestEntry(9, 2));
        leftSideAttributes.put("aInteger", lIntegerAttributes);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));

        ADBPartitionJoinTask task = ADBPartitionJoinTask.builder()
                                                        .leftPartitionManager(leftPartitionManager.ref())
                                                        .leftPartitionId(lPartitionId)
                                                        .rightPartitionManager(rightPartitionManager.ref())
                                                        .rightPartitionId(rPartitionId)
                                                        .build();

        Behavior<ADBPartitionJoinExecutor.Command> behavior = ADBPartitionJoinExecutorFactory
                .createDefault(joinQuery, supervisor.ref());
        ActorRef<ADBPartitionJoinExecutor.Command> executor = testKit.spawn(behavior);

        executor.tell(new ADBPartitionJoinExecutor.Prepare(task));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesLeft =
                leftPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(requestJoinAttributesLeft.getMessage().getAttributes()).containsAll(joinQuery.getAllLeftHandSideFields());
        executor.tell(new ADBPartition.MultipleAttributes(leftSideAttributes));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesRight =
                rightPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(requestJoinAttributesRight.getMessage().getAttributes()).containsAll(joinQuery.getAllRightHandSideFields());

        executor.tell(new ADBPartition.MultipleAttributes(rightSideAttributes));

        executor.tell(new ADBPartitionJoinExecutor.Execute());

        ADBPartitionJoinExecutor.JoinTaskPrepared responsePrepared =
                supervisor.expectMessageClass(ADBPartitionJoinExecutor.JoinTaskPrepared.class);

        assertThat(responsePrepared.getRespondTo()).isEqualTo(executor);

        ADBPartitionJoinExecutor.PartitionsJoined responseResults =
                supervisor.expectMessageClass(ADBPartitionJoinExecutor.PartitionsJoined.class);

        assertThat(responseResults.getJoinTuples().size()).isEqualTo(1);
        assertThat(ADBInternalIDHelper.getEntityId(responseResults.getJoinTuples().get(0).getKey())).isEqualTo(0);
        assertThat(ADBInternalIDHelper.getEntityId(responseResults.getJoinTuples().get(0).getValue())).isEqualTo(2);
    }

    @Test
    public void expectGreaterReversedJoinToBeExecutedSuccessfully() {
        int lPartitionId = 0;
        int rPartitionId = 0;

        TestProbe<ADBPartitionJoinExecutor.Response> supervisor = testKit.createTestProbe();

        TestProbe<ADBPartitionManager.Command> leftPartitionManager = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> rightPartitionManager = testKit.createTestProbe();

        Map<String, ObjectList<ADBEntityEntry>> rightSideAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBEntityEntry> lIntegerAttributes = new ObjectArrayList<>();
        lIntegerAttributes.add(this.createTestEntry(3, 0));
        lIntegerAttributes.add(this.createTestEntry(5, 1));
        lIntegerAttributes.add(this.createTestEntry(9, 2));
        rightSideAttributes.put("bInteger", lIntegerAttributes);

        Map<String, ObjectList<ADBEntityEntry>> leftSideAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBEntityEntry> fIntegerAttributes = new ObjectArrayList<>();
        fIntegerAttributes.add(this.createTestEntry(1, 0));
        fIntegerAttributes.add(this.createTestEntry(3, 1));
        fIntegerAttributes.add(this.createTestEntry(5, 2));
        leftSideAttributes.put("aInteger", fIntegerAttributes);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.GREATER, "aInteger", "bInteger"));

        Behavior<ADBPartitionJoinExecutor.Command> behavior = ADBPartitionJoinExecutorFactory
                .createDefault(joinQuery, supervisor.ref());
        ActorRef<ADBPartitionJoinExecutor.Command> executor = testKit.spawn(behavior);

        ADBPartitionJoinTask task = ADBPartitionJoinTask.builder()
                                                        .leftPartitionManager(leftPartitionManager.ref())
                                                        .leftPartitionId(lPartitionId)
                                                        .rightPartitionManager(rightPartitionManager.ref())
                                                        .rightPartitionId(rPartitionId)
                                                        .build();

        executor.tell(new ADBPartitionJoinExecutor.Prepare(task));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesLeft =
                leftPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);
        assertThat(requestJoinAttributesLeft.getMessage().getAttributes()).containsAll(joinQuery.getAllLeftHandSideFields());
        executor.tell(new ADBPartition.MultipleAttributes(leftSideAttributes));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesRight =
                rightPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);
        assertThat(requestJoinAttributesRight.getMessage().getAttributes()).containsAll(joinQuery.getAllRightHandSideFields());
        executor.tell(new ADBPartition.MultipleAttributes(rightSideAttributes));

        ADBPartitionJoinExecutor.JoinTaskPrepared responsePrepared =
                supervisor.expectMessageClass(ADBPartitionJoinExecutor.JoinTaskPrepared.class);

        assertThat(responsePrepared.getRespondTo()).isEqualTo(executor);

        executor.tell(new ADBPartitionJoinExecutor.Execute());

        ADBPartitionJoinExecutor.PartitionsJoined responseResults =
                supervisor.expectMessageClass(ADBPartitionJoinExecutor.PartitionsJoined.class);

        assertThat(responseResults.getJoinTuples().size()).isEqualTo(1);
        assertThat(ADBInternalIDHelper.getEntityId(responseResults.getJoinTuples().get(0).getKey())).isEqualTo(2);
        assertThat(ADBInternalIDHelper.getEntityId(responseResults.getJoinTuples().get(0).getValue())).isEqualTo(0);
    }

    @Test
    public void expectJoinWithToTermsToBeExecutedSuccessfully() {
        int lPartitionId = 0;
        int rPartitionId = 0;

        TestProbe<ADBPartitionJoinExecutor.Response> supervisor = testKit.createTestProbe();

        TestProbe<ADBPartitionManager.Command> leftPartitionManager = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> rightPartitionManager = testKit.createTestProbe();

        Map<String, ObjectList<ADBEntityEntry>> leftSideAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBEntityEntry> fIntegerAttributes = new ObjectArrayList<>();
        fIntegerAttributes.add(this.createTestEntry(1, 0));
        fIntegerAttributes.add(this.createTestEntry(3, 1));
        fIntegerAttributes.add(this.createTestEntry(6, 2));
        leftSideAttributes.put("aInteger", fIntegerAttributes);

        Map<String, ObjectList<ADBEntityEntry>> rightSideAttributes = new Object2ObjectHashMap<>();
        ObjectList<ADBEntityEntry> lIntegerAttributes = new ObjectArrayList<>();
        lIntegerAttributes.add(this.createTestEntry(3, 0));
        lIntegerAttributes.add(this.createTestEntry(5, 1));
        lIntegerAttributes.add(this.createTestEntry(9, 2));
        rightSideAttributes.put("aInteger", lIntegerAttributes);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL, "aInteger", "aInteger"));
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL, "aInteger", "aInteger"));

        Behavior<ADBPartitionJoinExecutor.Command> behavior = ADBPartitionJoinExecutorFactory
                .createDefault(joinQuery, supervisor.ref());
        ActorRef<ADBPartitionJoinExecutor.Command> executor = testKit.spawn(behavior);

        ADBPartitionJoinTask task = ADBPartitionJoinTask.builder()
                                                        .leftPartitionManager(leftPartitionManager.ref())
                                                        .leftPartitionId(lPartitionId)
                                                        .rightPartitionManager(rightPartitionManager.ref())
                                                        .rightPartitionId(rPartitionId)
                                                        .build();

        executor.tell(new ADBPartitionJoinExecutor.Prepare(task));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesLeft =
                leftPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);
        assertThat(requestJoinAttributesLeft.getMessage().getAttributes()).containsAll(joinQuery.getAllLeftHandSideFields());
        executor.tell(new ADBPartition.MultipleAttributes(leftSideAttributes));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesRight =
                rightPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(requestJoinAttributesRight.getMessage().getAttributes()).containsAll(joinQuery.getAllRightHandSideFields());

        executor.tell(new ADBPartition.MultipleAttributes(rightSideAttributes));

        ADBPartitionJoinExecutor.JoinTaskPrepared responsePrepared =
                supervisor.expectMessageClass(ADBPartitionJoinExecutor.JoinTaskPrepared.class);

        assertThat(responsePrepared.getRespondTo()).isEqualTo(executor);

        executor.tell(new ADBPartitionJoinExecutor.Execute());

        ADBPartitionJoinExecutor.PartitionsJoined responseResults =
                supervisor.expectMessageClass(ADBPartitionJoinExecutor.PartitionsJoined.class);

        assertThat(responseResults.getJoinTuples().size()).isEqualTo(1);
        assertThat(ADBInternalIDHelper.getEntityId(responseResults.getJoinTuples().get(0).getKey())).isEqualTo(1);
        assertThat(ADBInternalIDHelper.getEntityId(responseResults.getJoinTuples().get(0).getValue())).isEqualTo(0);
    }


}