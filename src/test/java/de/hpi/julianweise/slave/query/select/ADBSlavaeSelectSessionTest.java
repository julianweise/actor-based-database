package de.hpi.julianweise.slave.query.select;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.master.data_loading.distribution.ADBDataDistributor;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.master.query.select.ADBMasterSelectSession;
import de.hpi.julianweise.query.selection.ADBSelectionQuery;
import de.hpi.julianweise.query.selection.ADBSelectionQueryPredicate;
import de.hpi.julianweise.query.selection.constant.ADBPredicateIntConstant;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.ADBPartitionManagerFactory;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySessionFactory;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;

import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.EQUALITY;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBSlavaeSelectSessionTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @Before
    public void setUp() {
        testKit.spawn(ADBSlave.create());
        ADBComparator.buildComparatorMapping();
    }

    @After
    public void cleanup() {
        testKit.after();
        testKit = new TestKitJunitResource();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
        System.gc();
    }

    @AfterClass
    public static void after() {
        testKit.after();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
    }

    @Test
    public void returnEmptyResults() {
        int transactionId = 1;

        TestProbe<ADBMasterQuerySession.Command> responseProbe = testKit.createTestProbe();

        ADBSelectionQuery query = new ADBSelectionQuery();
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(new ADBPredicateIntConstant(1))
                .build();
        query.addPredicate(predicate);

        ADBQueryManager.QueryEntities message = new ADBQueryManager.QueryEntities(transactionId, responseProbe.ref(), query);

        Behavior<ADBSlaveQuerySession.Command> selectBehavior = ADBSlaveQuerySessionFactory.create(message);


        ActorRef<ADBSlaveQuerySession.Command> selectHandler = testKit.spawn(selectBehavior, "select-handler");
        selectHandler.tell(new ADBSlaveQuerySession.Execute());

        selectHandler.tell(new ADBSlaveQuerySession.MessageSenderResponse(new ADBLargeMessageSender.TransferCompleted()));

        ADBMasterQuerySession.RegisterQuerySessionHandler handlerRegistration =
                responseProbe.expectMessageClass(ADBMasterQuerySession.RegisterQuerySessionHandler.class);

        ADBMasterQuerySession.ConcludeTransaction conclusion =
                responseProbe.expectMessageClass(ADBMasterQuerySession.ConcludeTransaction.class);

        assertThat(handlerRegistration.getSessionHandler()).isEqualTo(selectHandler);
        assertThat(conclusion.getSlaveQuerySession()).isEqualTo(selectHandler);
    }

    @Test
    public void returnValidResultSet() {
        TestProbe<ADBDataDistributor.Command> persistProbe = testKit.createTestProbe();
        TestProbe<Receptionist.Listing> receptionistProbe = testKit.createTestProbe();

        int transactionId = 1;
        TestProbe<ADBMasterQuerySession.Command> responseProbe = testKit.createTestProbe();

        testKit.system().receptionist().tell(Receptionist.subscribe(ADBPartitionManager.SERVICE_KEY,
                receptionistProbe.ref()));

        Receptionist.Listing emptyListing = receptionistProbe.expectMessageClass(Receptionist.Listing.class);
        assertThat(emptyListing.getAllServiceInstances(ADBPartitionManager.SERVICE_KEY).size()).isZero();
        Receptionist.Listing validListing = receptionistProbe.expectMessageClass(Receptionist.Listing.class);
        assertThat(validListing.getAllServiceInstances(ADBPartitionManager.SERVICE_KEY).size()).isOne();

        ADBEntity testEntity = new TestEntity(1, "Test", 2f, true, 12.02132);
        validListing.getAllServiceInstances(ADBPartitionManager.SERVICE_KEY).forEach(manager ->
                manager.tell(new ADBPartitionManager.PersistEntities(Adapter.toClassic(persistProbe.ref()),
                        new ObjectArrayList<>(Collections.singletonList(testEntity)))));
        validListing.getAllServiceInstances(ADBPartitionManager.SERVICE_KEY).forEach(manager ->
                manager.tell(new ADBPartitionManager.ConcludeTransfer()));

        ADBSelectionQuery query = new ADBSelectionQuery();
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(new ADBPredicateIntConstant(1))
                .build();
        query.addPredicate(predicate);


        ADBQueryManager.QueryEntities message = new ADBQueryManager.QueryEntities(transactionId, responseProbe.ref(), query);

        Behavior<ADBSlaveQuerySession.Command> selectBehavior = ADBSlaveQuerySessionFactory.create(message);

        ActorRef<ADBSlaveQuerySession.Command> selectHandler = testKit.spawn(selectBehavior, "select-handler");
        selectHandler.tell(new ADBSlaveQuerySession.Execute());

        ADBMasterQuerySession.RegisterQuerySessionHandler handlerRegistration =
                responseProbe.expectMessageClass(ADBMasterQuerySession.RegisterQuerySessionHandler.class);

        assertThat(handlerRegistration.getSessionHandler()).isEqualTo(selectHandler);

        ADBLargeMessageReceiver.InitializeTransfer initializeTransfer =
                responseProbe.expectMessageClass(ADBLargeMessageReceiver.InitializeTransfer.class);

        assertThat(initializeTransfer.getType()).isEqualTo(ADBMasterSelectSession.SelectQueryResults.class);
        assertThat(initializeTransfer.getTotalSize()).isGreaterThan(0);

        selectHandler.tell(new ADBSlaveQuerySession.MessageSenderResponse(new ADBLargeMessageSender.TransferCompleted()));

        ADBMasterQuerySession.ConcludeTransaction conclusion = (ADBMasterQuerySession.ConcludeTransaction) responseProbe
                .receiveMessage();

        assertThat(conclusion.getSlaveQuerySession()).isEqualTo(selectHandler);
    }

}