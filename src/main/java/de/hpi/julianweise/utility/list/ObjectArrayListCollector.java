package de.hpi.julianweise.utility.list;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class ObjectArrayListCollector<T> implements Collector<T, ObjectArrayList<T>, ObjectArrayList<T>> {
    @Override
    public Supplier<ObjectArrayList<T>> supplier() {
        return ObjectArrayList::new;
    }

    @Override
    public BiConsumer<ObjectArrayList<T>, T> accumulator() {
        return ObjectArrayList::add;
    }

    @Override
    public BinaryOperator<ObjectArrayList<T>> combiner() {
        return (c1, c2) -> {
            c1.addAll(c2);
            return c1;
        };
    }

    @Override
    public Function<ObjectArrayList<T>, ObjectArrayList<T>> finisher() {
        return x -> x;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return new HashSet<Characteristics>() {{
            add(Characteristics.IDENTITY_FINISH);
        }};
    }
}
