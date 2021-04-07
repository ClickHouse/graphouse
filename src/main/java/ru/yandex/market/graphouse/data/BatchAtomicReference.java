package ru.yandex.market.graphouse.data;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public class BatchAtomicReference<T> extends AtomicReference<T> {

    /**
     * Atomically updates the current value with the results of
     * applying the given function, returning the updated value. The
     * functions may be re-applied when attempted updates fail due to contention among threads
     * or received false value from {@param checkUpdatePermissionFunction}
     *
     * @param finalizeAfterRejectedFunction function to clear the new value from the previous update attempt
     * @param updateFunction                a side-effect-free function
     * @param checkSetPermissionFunction    checking the permission before setting a new value
     * @param postUpdateFunction            function for an action with a new value after update
     * @return the updated value
     */
    public final T updateAndGetBatch(
        UnaryOperator<T> updateFunction,
        Function<T, Boolean> checkSetPermissionFunction,
        Consumer<T> finalizeAfterRejectedFunction,
        BiConsumer<T, Boolean> postUpdateFunction
    ) {
        boolean allowToSet, newBatchCreated;
        T prev, next = null;
        do {
            if (next != null) {
                finalizeAfterRejectedFunction.accept(next);
            }
            prev = get();
            next = updateFunction.apply(prev);
            newBatchCreated = prev != next;
            allowToSet = checkSetPermissionFunction.apply(next);
        } while (!allowToSet || !compareAndSet(prev, next));

        postUpdateFunction.accept(next, newBatchCreated);

        return next;
    }
}
