package de.hpi.julianweise.partition;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.csv.TestEntityFactory;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.master.data_loading.distribution.ADBDataDistributor;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.master.query_endpoint.ADBPartitionInquirer;
import de.hpi.julianweise.master.query_endpoint.ADBPartitionInquirerFactory;
import de.hpi.julianweise.query.selection.ADBSelectionQuery;
import de.hpi.julianweise.query.selection.ADBSelectionQueryPredicate;
import de.hpi.julianweise.query.selection.constant.ADBPredicateIntConstant;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.EQUALITY;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBPartitionInquirerTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @Before
    public void setUp() {
        ADBEntityFactoryProvider.initialize(new TestEntityFactory());
        testKit.spawn(ADBSlave.create());
        ADBComparator.buildComparatorMapping();
    }

    @After
    public void cleanup() {
        testKit.after();
        testKit = new TestKitJunitResource();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetPool();
        ADBQueryManager.resetSingleton();
    }

    @AfterClass
    public static void after() {
        testKit.after();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
    }

    @Test
    public void testDistributeQuerySuccessfully() {
        int requestId = 1;
        TestProbe<ADBPartitionInquirer.QueryConclusion> resultProbe = testKit.createTestProbe();

        TestProbe<Receptionist.Listing> receptionistProbe = testKit.createTestProbe();
        testKit.system().receptionist().tell(Receptionist.subscribe(ADBQueryManager.SERVICE_KEY, receptionistProbe.ref()));

        TestProbe<ADBQueryManager.Command> testProbe = testKit.createTestProbe();
        testKit.system().receptionist().tell(Receptionist.register(ADBQueryManager.SERVICE_KEY, testProbe.ref()));
        ActorRef<ADBPartitionInquirer.Command> inquirer = testKit.spawn(ADBPartitionInquirerFactory.createDefault());

        // necessary to ensure receptionist registration propagates successfully before querying nodes
        receptionistProbe.receiveSeveralMessages(2);

        ADBSelectionQuery query = new ADBSelectionQuery();

        inquirer.tell(ADBPartitionInquirer.QueryNodes.builder()
                                                     .requestId(requestId)
                                                     .query(query)
                                                     .respondTo(resultProbe.ref())
                                                     .build());

        ADBQueryManager.Command queryCommand = testProbe.receiveMessage();
        ADBQueryManager.QueryEntities queryForNodes = (ADBQueryManager.QueryEntities) queryCommand;

        assertThat(queryForNodes.getQuery()).isEqualTo(query);
    }

    @Test
    public void testDistributeQueryToManyNodesSuccessfully() {
        int requestId = 1;

        TestProbe<ADBPartitionInquirer.QueryConclusion> resultProbe = testKit.createTestProbe();

        TestProbe<Receptionist.Listing> receptionistProbe = testKit.createTestProbe();
        testKit.system().receptionist().tell(Receptionist.subscribe(ADBQueryManager.SERVICE_KEY, receptionistProbe.ref()));

        TestProbe<ADBQueryManager.Command> testProbe1 = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> testProbe2 = testKit.createTestProbe();
        testKit.system().receptionist().tell(Receptionist.register(ADBQueryManager.SERVICE_KEY, testProbe1.ref()));
        testKit.system().receptionist().tell(Receptionist.register(ADBQueryManager.SERVICE_KEY, testProbe2.ref()));
        ActorRef<ADBPartitionInquirer.Command> inquirer = testKit.spawn(ADBPartitionInquirerFactory.createDefault());

        // necessary to ensure receptionist registration propagates successfully before querying nodes
        receptionistProbe.receiveSeveralMessages(2);

        ADBSelectionQuery query = new ADBSelectionQuery();

        inquirer.tell(ADBPartitionInquirer.QueryNodes.builder()
                                                     .requestId(requestId)
                                                     .query(query)
                                                     .respondTo(resultProbe.ref())
                                                     .build());

        ADBQueryManager.Command queryCommand1 = testProbe1.receiveMessage();
        ADBQueryManager.QueryEntities queryForNodes1 = (ADBQueryManager.QueryEntities) queryCommand1;

        ADBQueryManager.Command queryCommand2 = testProbe2.receiveMessage();
        ADBQueryManager.QueryEntities queryForNodes2 = (ADBQueryManager.QueryEntities) queryCommand2;

        assertThat(queryForNodes1.getQuery()).isEqualTo(query);
        assertThat(queryForNodes2.getQuery()).isEqualTo(query);
    }

    @Test
    public void testEmptyResultsAreReturnedSuccessfully() {
        int requestId = 1;
        TestProbe<ADBDataDistributor.Command> persistProbe = testKit.createTestProbe();

        // Implicitly ensure that test waits for receptionist registrations to propagate
        TestProbe<Receptionist.Listing> receptionistProbe = testKit.createTestProbe();
        TestProbe<Receptionist.Listing> receptionistProbe2 = testKit.createTestProbe();
        testKit.system().receptionist().tell(Receptionist.subscribe(ADBQueryManager.SERVICE_KEY, receptionistProbe.ref()));

        testKit.system().receptionist().tell(Receptionist.subscribe(ADBPartitionManager.SERVICE_KEY,
                receptionistProbe2.ref()));

        // Ensure node is present
        ADBEntity testEntity = new TestEntity(1, "Test", 2f, true, 12.02132);

        TestProbe<ADBPartitionInquirer.QueryConclusion> resultProbe = testKit.createTestProbe();
        ActorRef<ADBPartitionInquirer.Command> inquirer = testKit.spawn(ADBPartitionInquirerFactory.createDefault());

        // necessary to ensure receptionist registration propagates successfully before querying nodes
        receptionistProbe.receiveSeveralMessages(2);

        Receptionist.Listing listing2 = receptionistProbe2.expectMessageClass(Receptionist.Listing.class);
        assertThat(listing2.getAllServiceInstances(ADBPartitionManager.SERVICE_KEY).size()).isZero();

        listing2 = receptionistProbe2.expectMessageClass(Receptionist.Listing.class);
        assertThat(listing2.getAllServiceInstances(ADBPartitionManager.SERVICE_KEY).size()).isOne();

        listing2.getAllServiceInstances(ADBPartitionManager.SERVICE_KEY).forEach(manager ->
                manager.tell(new ADBPartitionManager.PersistEntity(persistProbe.ref(), testEntity)));
        listing2.getAllServiceInstances(ADBPartitionManager.SERVICE_KEY).forEach(manager ->
                manager.tell(new ADBPartitionManager.ConcludeTransfer()));

        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addPredicate(new ADBSelectionQueryPredicate(new ADBPredicateIntConstant(11212), "aInteger", EQUALITY));
        inquirer.tell(ADBPartitionInquirer.QueryNodes.builder()
                                                     .requestId(requestId)
                                                     .query(query)
                                                     .respondTo(resultProbe.ref())
                                                     .build());

        ADBPartitionInquirer.QueryConclusion results = resultProbe.receiveMessage();

        assertThat(results.getResultsCount()).isZero();
    }

    @Test
    public void ensureNoArgsConstructorForConcludeTransactionForDeserializationIsPresent() {
        ADBMasterQuerySession.ConcludeTransaction concludeTransaction = new ADBMasterQuerySession.ConcludeTransaction();

        assertThat(concludeTransaction.getSlaveQuerySession()).isNull();
    }


}