package de.hpi.julianweise.utility.query.join;

import akka.actor.typed.ActorRef;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import it.unimi.dsi.fastutil.ints.AbstractInt2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JoinDistributionPlan {

    private final static Logger LOG = LoggerFactory.getLogger(JoinDistributionPlan.class);

    private final ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers;
    private final BitSet[] distributionMap;
    private final Int2ObjectMap<AtomicInteger> dataAccesses = new Int2ObjectOpenHashMap<>();
    private final Int2ObjectMap<AtomicInteger> dataRequests = new Int2ObjectOpenHashMap<>();
    private final AtomicInteger finalizedComparisons = new AtomicInteger(0);

    public JoinDistributionPlan(ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers) {
        this.queryManagers = queryManagers;
        this.distributionMap = this.initializeDistributionMap(this.queryManagers.size());
        IntStream.range(0, this.queryManagers.size()).forEach(index -> this.dataAccesses.put(index, new AtomicInteger()));
        IntStream.range(0, this.queryManagers.size()).forEach(index -> this.dataRequests.put(index, new AtomicInteger()));
    }

    private BitSet[] initializeDistributionMap(int numberOfNodes) {
        BitSet[] map = new BitSet[numberOfNodes];
        for (int i = 0; i < numberOfNodes; i++) {
            map[i] = new BitSet(numberOfNodes);
            for (int j = 0; j <= i; j++) {
                map[i].set(j);
            }
        }
        return map;
    }

    public ActorRef<ADBQueryManager.Command> getNextJoinNodeFor(ActorRef<ADBQueryManager.Command> queryManager) {
        int nodeIndex = this.getIndexOfNode(queryManager);
        int nodeIndexMinimalAccesses = this.getNodeIndexWithMinimalAccessesForNode(nodeIndex);
        LOG.info(String.format("[DistributionPlan] Node #%d requested new Node to join. Suggested Node # %d",
                nodeIndex, nodeIndexMinimalAccesses));
        LOG.info("[Overall Process]: {}/{}", this.finalizedComparisons.incrementAndGet(),
                this.queryManagers.size() * (this.queryManagers.size() + 1) / 2);
        if (nodeIndexMinimalAccesses < 0) {
            return null;
        }
        this.distributionMap[nodeIndex].set(nodeIndexMinimalAccesses);
        this.distributionMap[nodeIndexMinimalAccesses].set(nodeIndex);
        this.dataAccesses.get(nodeIndex).incrementAndGet();
        this.dataAccesses.get(nodeIndexMinimalAccesses).incrementAndGet();
        this.dataRequests.get(nodeIndex).incrementAndGet();
        return this.queryManagers.get(nodeIndexMinimalAccesses);
    }

    private int getIndexOfNode(ActorRef<ADBQueryManager.Command> node) {
        return this.queryManagers.indexOf(node);
    }

    private int getNodeIndexWithMinimalAccessesForNode(int index) {
        IntList relevantAccesses = this.dataAccesses
                .int2ObjectEntrySet().stream().filter(eS -> this.notCompared(eS.getIntKey(), index)).map(eS -> eS.getValue().get()).collect(Collectors.toCollection(IntArrayList::new));
        int minDataAccesses = relevantAccesses.size() > 0 ? Collections.min(relevantAccesses) : Integer.MAX_VALUE;
        return this.dataAccesses.int2ObjectEntrySet()
                                .stream()
                                .sorted(Comparator.comparingInt(Int2ObjectMap.Entry::getIntKey))
                                .filter(eS -> this.notCompared(eS.getIntKey(), index))
                                .filter(eS1 -> eS1.getValue().get() <= minDataAccesses)
                                .min((eS1, eS2) -> this.dataRequests.get(eS2.getIntKey()).get() - this.dataRequests.get(eS1.getIntKey()).get())
                                .orElseGet(() -> new AbstractInt2ObjectMap.BasicEntry<>(-1, new AtomicInteger(-1)))
                                .getIntKey();
    }

    private boolean notCompared(int indexA, int indexB) {
        return !(this.distributionMap[indexA].get(indexB) && this.distributionMap[indexB].get(indexA));
    }
}
