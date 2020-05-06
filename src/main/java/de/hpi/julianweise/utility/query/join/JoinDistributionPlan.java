package de.hpi.julianweise.utility.query.join;

import akka.actor.typed.ActorRef;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JoinDistributionPlan {

    private final static Logger LOG = LoggerFactory.getLogger(JoinDistributionPlan.class);

    private final List<ActorRef<ADBQueryManager.Command>> queryManager;
    private final BitSet[] distributionMap;
    private final Map<Integer, AtomicInteger> dataAccesses = new HashMap<>();
    private final Map<Integer, AtomicInteger> dataRequests = new HashMap<>();
    private final AtomicInteger finalizedComparisons = new AtomicInteger(0);

    public JoinDistributionPlan(List<ActorRef<ADBQueryManager.Command>> queryManager) {
        this.queryManager = queryManager;
        this.distributionMap = this.initializeDistributionMap(this.queryManager.size());
        IntStream.range(0, this.queryManager.size()).forEach(index -> this.dataAccesses.put(index, new AtomicInteger()));
        IntStream.range(0, this.queryManager.size()).forEach(index -> this.dataRequests.put(index, new AtomicInteger()));
    }

    private BitSet[] initializeDistributionMap(int numberOfShards) {
        BitSet[] map = new BitSet[numberOfShards];
        for (int i = 0; i < numberOfShards; i++) {
            map[i] = new BitSet(numberOfShards);
            for (int j = 0; j <= i; j++) {
                map[i].set(j);
            }
        }
        return map;
    }

    public ActorRef<ADBQueryManager.Command> getNextJoinShardFor(ActorRef<ADBQueryManager.Command> queryManager) {
        int shardIndex = this.getIndexOfShard(queryManager);
        int shardIndexMinimalAccesses = this.getShardIndexWithMinimalAccessesForShard(shardIndex);
        LOG.info(String.format("[DistributionPlan] Shard #%d requested new shard to join. Suggested shard # %d",
                shardIndex, shardIndexMinimalAccesses));
        LOG.info("[Overall Process]: {}/{}", this.finalizedComparisons.incrementAndGet(),
                this.queryManager.size() * (this.queryManager.size() + 1) / 2);
        if (shardIndexMinimalAccesses < 0) {
            return null;
        }
        this.distributionMap[shardIndex].set(shardIndexMinimalAccesses);
        this.distributionMap[shardIndexMinimalAccesses].set(shardIndex);
        this.dataAccesses.get(shardIndex).incrementAndGet();
        this.dataAccesses.get(shardIndexMinimalAccesses).incrementAndGet();
        this.dataRequests.get(shardIndex).incrementAndGet();
        return this.queryManager.get(shardIndexMinimalAccesses);
    }

    private int getIndexOfShard(ActorRef<ADBQueryManager.Command> shard) {
        return this.queryManager.indexOf(shard);
    }

    private int getShardIndexWithMinimalAccessesForShard(int index) {
        IntList relevantAccesses = this.dataAccesses
                .entrySet().stream().filter(eS -> this.notCompared(eS.getKey(), index)).map(eS -> eS.getValue().get()).collect(Collectors.toCollection(IntArrayList::new));
        int minDataAccesses = relevantAccesses.size() > 0 ? Collections.min(relevantAccesses) : Integer.MAX_VALUE;
        return this.dataAccesses.entrySet()
                                .stream()
                                .filter(eS -> this.notCompared(eS.getKey(), index))
                                .filter(eS1 -> eS1.getValue().get() <= minDataAccesses)
                                .min((eS1, eS2) -> this.dataRequests.get(eS2.getKey()).get() - this.dataRequests.get(eS1.getKey()).get())
                                .orElseGet(() -> new AbstractMap.SimpleEntry<>(-1, new AtomicInteger(-1)))
                                .getKey();
    }

    private boolean notCompared(int indexA, int indexB) {
        return !(this.distributionMap[indexA].get(indexB) && this.distributionMap[indexB].get(indexA));
    }
}
