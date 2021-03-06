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
import de.hpi.julianweise.slave.partition.column.sorted.ADBColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBDoubleColumnSorted;
import de.hpi.julianweise.slave.partition.column.sorted.ADBIntColumnSorted;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntryFactory;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.join.node.ADBPartitionJoinTask;
import de.hpi.julianweise.utility.internals.ADBInternalIDHelper;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
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

        Map<String, ADBColumnSorted> leftSideAttributes = new Object2ObjectOpenHashMap<>();
        leftSideAttributes.put("aInteger", new ADBIntColumnSorted(0, lPartitionId, new int[]{1, 3, 5}, new int[]{0, 1
                , 2}, new int[]{0, 1, 2}));

        Map<String, ADBColumnSorted> rightSideAttributes = new Object2ObjectOpenHashMap<>();
        rightSideAttributes.put("aInteger", new ADBIntColumnSorted(0, lPartitionId, new int[]{3, 5, 9}, new int[]{0,
                1, 2}, new int[]{0, 1, 2}));

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        Behavior<ADBPartitionJoinExecutor.Command> behavior = ADBPartitionJoinExecutorFactory
                .createDefault(joinQuery, supervisor.ref());
        ActorRef<ADBPartitionJoinExecutor.Command> executor = testKit.spawn(behavior);

        ADBPartitionHeader headerLeft = new ADBPartitionHeader(
                Collections.singletonMap("aInteger", this.createTestEntry(1, 0)),
                Collections.singletonMap("aInteger", this.createTestEntry(5, 2)),
                lPartitionId);

        ADBPartitionHeader headerRight = new ADBPartitionHeader(
                Collections.singletonMap("aInteger", this.createTestEntry(3, 0)),
                Collections.singletonMap("aInteger", this.createTestEntry(9, 2)),
                rPartitionId);

        Object2IntMap<String> originalSizes = new Object2IntOpenHashMap<>();
        originalSizes.put("aInteger", 2);

        ADBPartitionJoinTask task = ADBPartitionJoinTask.builder()
                                                        .leftPartitionManager(leftPartitionManager.ref())
                                                        .leftPartitionId(lPartitionId)
                                                        .rightPartitionManager(rightPartitionManager.ref())
                                                        .rightPartitionId(rPartitionId)
                                                        .leftHeader(headerLeft)
                                                        .rightHeader(headerRight)
                                                        .build();

        executor.tell(new ADBPartitionJoinExecutor.Prepare(task));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesLeft =
                leftPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(requestJoinAttributesLeft.getMessage().getAttributes()).containsAll(Arrays.asList(joinQuery.getAllLeftHandSideFields()));

        executor.tell(new ADBPartition.MultipleAttributes(leftSideAttributes, originalSizes, true));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesRight =
                rightPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(requestJoinAttributesRight.getMessage().getAttributes()).containsAll(Arrays.asList(joinQuery.getAllRightHandSideFields()));

        executor.tell(new ADBPartition.MultipleAttributes(rightSideAttributes, originalSizes, false));

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
    public void expectLessReversedDoubleJoinToBeExecutedSuccessfully() {
        int lPartitionId = 0;
        int rPartitionId = 0;

        TestProbe<ADBPartitionJoinExecutor.Response> supervisor = testKit.createTestProbe();

        TestProbe<ADBPartitionManager.Command> leftPartitionManager = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> rightPartitionManager = testKit.createTestProbe();

        Map<String, ADBColumnSorted> rightSideAttributes = new Object2ObjectOpenHashMap<>();
        rightSideAttributes.put("eDouble", new ADBDoubleColumnSorted(0, lPartitionId, new double[]{1, 3, 5}, new int[]{0, 1, 2}, new int[]{0, 1, 2}));

        Map<String, ADBColumnSorted> leftSideAttributes = new Object2ObjectOpenHashMap<>();
        leftSideAttributes.put("eDouble", new ADBDoubleColumnSorted(0, lPartitionId, new double[]{3, 5, 9}, new int[]{0, 1, 2},   new int[]{0, 1, 2}));

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS, "eDouble", "eDouble"));

        ADBPartitionHeader headerLeft = new ADBPartitionHeader(
                Collections.singletonMap("eDouble", this.createTestEntry(1, 0)),
                Collections.singletonMap("eDouble", this.createTestEntry(5, 2)),
                lPartitionId);

        ADBPartitionHeader headerRight = new ADBPartitionHeader(
                Collections.singletonMap("eDouble", this.createTestEntry(3, 0)),
                Collections.singletonMap("eDouble", this.createTestEntry(9, 2)),
                rPartitionId);

        Object2IntMap<String> originalSizes = new Object2IntOpenHashMap<>();
        originalSizes.put("eDouble", 2);

        ADBPartitionJoinTask task = ADBPartitionJoinTask.builder()
                                                        .leftPartitionManager(leftPartitionManager.ref())
                                                        .leftPartitionId(lPartitionId)
                                                        .rightPartitionManager(rightPartitionManager.ref())
                                                        .rightPartitionId(rPartitionId)
                                                        .leftHeader(headerLeft)
                                                        .rightHeader(headerRight)
                                                        .build();

        Behavior<ADBPartitionJoinExecutor.Command> behavior = ADBPartitionJoinExecutorFactory
                .createDefault(joinQuery, supervisor.ref());
        ActorRef<ADBPartitionJoinExecutor.Command> executor = testKit.spawn(behavior);

        executor.tell(new ADBPartitionJoinExecutor.Prepare(task));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesLeft =
                leftPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(requestJoinAttributesLeft.getMessage().getAttributes()).containsAll(Arrays.asList(joinQuery.getAllLeftHandSideFields()));
        executor.tell(new ADBPartition.MultipleAttributes(leftSideAttributes, originalSizes, true));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesRight =
                rightPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(requestJoinAttributesRight.getMessage().getAttributes()).containsAll(Arrays.asList(joinQuery.getAllRightHandSideFields()));

        executor.tell(new ADBPartition.MultipleAttributes(rightSideAttributes, originalSizes, false));

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
    public void expectLessReversedJoinToBeExecutedSuccessfully() {
        int lPartitionId = 0;
        int rPartitionId = 0;

        TestProbe<ADBPartitionJoinExecutor.Response> supervisor = testKit.createTestProbe();

        TestProbe<ADBPartitionManager.Command> leftPartitionManager = testKit.createTestProbe();
        TestProbe<ADBPartitionManager.Command> rightPartitionManager = testKit.createTestProbe();

        Map<String, ADBColumnSorted> rightSideAttributes = new Object2ObjectOpenHashMap<>();
        rightSideAttributes.put("aInteger", new ADBIntColumnSorted(0, lPartitionId, new int[]{1, 3, 5}, new int[]{0, 1, 2},   new int[]{0, 1, 2}));

        Map<String, ADBColumnSorted> leftSideAttributes = new Object2ObjectOpenHashMap<>();
        leftSideAttributes.put("aInteger", new ADBIntColumnSorted(0, lPartitionId, new int[]{3, 5, 9}, new int[]{0, 1, 2},   new int[]{0, 1, 2}));

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));

        ADBPartitionHeader headerLeft = new ADBPartitionHeader(
                Collections.singletonMap("aInteger", this.createTestEntry(1, 0)),
                Collections.singletonMap("aInteger", this.createTestEntry(5, 2)),
                lPartitionId);

        ADBPartitionHeader headerRight = new ADBPartitionHeader(
                Collections.singletonMap("aInteger", this.createTestEntry(3, 0)),
                Collections.singletonMap("aInteger", this.createTestEntry(9, 2)),
                rPartitionId);

        Object2IntMap<String> originalSizes = new Object2IntOpenHashMap<>();
        originalSizes.put("aInteger", 2);

        ADBPartitionJoinTask task = ADBPartitionJoinTask.builder()
                                                        .leftPartitionManager(leftPartitionManager.ref())
                                                        .leftPartitionId(lPartitionId)
                                                        .rightPartitionManager(rightPartitionManager.ref())
                                                        .rightPartitionId(rPartitionId)
                                                        .leftHeader(headerLeft)
                                                        .rightHeader(headerRight)
                                                        .build();

        Behavior<ADBPartitionJoinExecutor.Command> behavior = ADBPartitionJoinExecutorFactory
                .createDefault(joinQuery, supervisor.ref());
        ActorRef<ADBPartitionJoinExecutor.Command> executor = testKit.spawn(behavior);

        executor.tell(new ADBPartitionJoinExecutor.Prepare(task));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesLeft =
                leftPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(requestJoinAttributesLeft.getMessage().getAttributes()).containsAll(Arrays.asList(joinQuery.getAllLeftHandSideFields()));
        executor.tell(new ADBPartition.MultipleAttributes(leftSideAttributes, originalSizes, true));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesRight =
                rightPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(requestJoinAttributesRight.getMessage().getAttributes()).containsAll(Arrays.asList(joinQuery.getAllRightHandSideFields()));

        executor.tell(new ADBPartition.MultipleAttributes(rightSideAttributes, originalSizes, false));

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

        Map<String, ADBColumnSorted> leftSideAttributes = new Object2ObjectOpenHashMap<>();
        leftSideAttributes.put("aInteger", new ADBIntColumnSorted(0, lPartitionId, new int[]{1, 3, 5}, new int[]{0, 1, 2},   new int[]{0, 1, 2}));

        Map<String, ADBColumnSorted> rightSideAttributes = new Object2ObjectOpenHashMap<>();
        rightSideAttributes.put("bInteger", new ADBIntColumnSorted(0, lPartitionId, new int[]{3, 5, 9}, new int[]{0, 1, 2},   new int[]{0, 1, 2}));

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.GREATER, "aInteger", "bInteger"));

        Behavior<ADBPartitionJoinExecutor.Command> behavior = ADBPartitionJoinExecutorFactory
                .createDefault(joinQuery, supervisor.ref());
        ActorRef<ADBPartitionJoinExecutor.Command> executor = testKit.spawn(behavior);

        ADBPartitionHeader headerLeft = new ADBPartitionHeader(
                Collections.singletonMap("aInteger", this.createTestEntry(1, 0)),
                Collections.singletonMap("aInteger", this.createTestEntry(5, 2)),
                lPartitionId);

        ADBPartitionHeader headerRight = new ADBPartitionHeader(
                Collections.singletonMap("aInteger", this.createTestEntry(3, 0)),
                Collections.singletonMap("aInteger", this.createTestEntry(9, 2)),
                rPartitionId);

        Object2IntMap<String> originalSizes = new Object2IntOpenHashMap<>();
        originalSizes.put("aInteger", 2);

        ADBPartitionJoinTask task = ADBPartitionJoinTask.builder()
                                                        .leftPartitionManager(leftPartitionManager.ref())
                                                        .leftPartitionId(lPartitionId)
                                                        .rightPartitionManager(rightPartitionManager.ref())
                                                        .rightPartitionId(rPartitionId)
                                                        .leftHeader(headerLeft)
                                                        .rightHeader(headerRight)
                                                        .build();

        executor.tell(new ADBPartitionJoinExecutor.Prepare(task));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesLeft =
                leftPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);
        assertThat(requestJoinAttributesLeft.getMessage().getAttributes()).containsAll(Arrays.asList(joinQuery.getAllLeftHandSideFields()));
        executor.tell(new ADBPartition.MultipleAttributes(leftSideAttributes, originalSizes, true));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesRight =
                rightPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);
        assertThat(requestJoinAttributesRight.getMessage().getAttributes()).containsAll(Arrays.asList(joinQuery.getAllRightHandSideFields()));
        executor.tell(new ADBPartition.MultipleAttributes(rightSideAttributes, originalSizes, false));

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

        Map<String, ADBColumnSorted> leftSideAttributes = new Object2ObjectOpenHashMap<>();
        leftSideAttributes.put("aInteger", new ADBIntColumnSorted(0, lPartitionId, new int[]{1, 3, 6}, new int[]{0, 1, 2},   new int[]{0, 1, 2}));

        Map<String, ADBColumnSorted> rightSideAttributes = new Object2ObjectOpenHashMap<>();
        rightSideAttributes.put("aInteger", new ADBIntColumnSorted(0, lPartitionId, new int[]{3, 5, 9}, new int[]{0, 1, 2},   new int[]{0, 1, 2}));


        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL, "aInteger", "aInteger"));
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL, "aInteger", "aInteger"));

        Behavior<ADBPartitionJoinExecutor.Command> behavior = ADBPartitionJoinExecutorFactory
                .createDefault(joinQuery, supervisor.ref());
        ActorRef<ADBPartitionJoinExecutor.Command> executor = testKit.spawn(behavior);

        ADBPartitionHeader headerLeft = new ADBPartitionHeader(
                Collections.singletonMap("aInteger", this.createTestEntry(1, 0)),
                Collections.singletonMap("aInteger", this.createTestEntry(5, 2)),
                lPartitionId);

        ADBPartitionHeader headerRight = new ADBPartitionHeader(
                Collections.singletonMap("aInteger", this.createTestEntry(3, 0)),
                Collections.singletonMap("aInteger", this.createTestEntry(9, 2)),
                rPartitionId);

        Object2IntMap<String> originalSizes = new Object2IntOpenHashMap<>();
        originalSizes.put("aInteger", 2);

        ADBPartitionJoinTask task = ADBPartitionJoinTask.builder()
                                                        .leftPartitionManager(leftPartitionManager.ref())
                                                        .leftPartitionId(lPartitionId)
                                                        .rightPartitionManager(rightPartitionManager.ref())
                                                        .rightPartitionId(rPartitionId)
                                                        .leftHeader(headerLeft)
                                                        .rightHeader(headerRight)
                                                        .build();

        executor.tell(new ADBPartitionJoinExecutor.Prepare(task));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesLeft =
                leftPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);
        assertThat(requestJoinAttributesLeft.getMessage().getAttributes()).containsAll(Arrays.asList(joinQuery.getAllLeftHandSideFields()));
        executor.tell(new ADBPartition.MultipleAttributes(leftSideAttributes, originalSizes, true));

        ADBPartitionManager.RedirectToPartition requestJoinAttributesRight =
                rightPartitionManager.expectMessageClass(ADBPartitionManager.RedirectToPartition.class);

        assertThat(requestJoinAttributesRight.getMessage().getAttributes()).containsAll(Arrays.asList(joinQuery.getAllRightHandSideFields()));

        executor.tell(new ADBPartition.MultipleAttributes(rightSideAttributes, originalSizes, false));

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