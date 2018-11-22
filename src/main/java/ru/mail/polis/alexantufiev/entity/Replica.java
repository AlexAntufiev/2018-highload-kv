package ru.mail.polis.alexantufiev.entity;

import java.util.Optional;

/**
 * Реплика.
 *
 * @author Aleksey Antufev
 * @version 1.1.0
 * @since 1.1.0 16.10.2018
 */
public class Replica {

    private final int countRequests;
    private final int countOfNodes;

    private Replica(int countRequests, int countOfNodes) {
        this.countRequests = countRequests;
        this.countOfNodes = countOfNodes;
    }

    public static Optional<Replica> of(
        String replica,
        int countOfNodes,
        boolean noReplica
    ) throws IndexOutOfBoundsException, IllegalArgumentException {
        if (noReplica) {
            return Optional.empty();
        } else if (replica == null || replica.isEmpty()) {
            int tempCountRequests = countOfNodes / 2 + 1;
            return Optional.of(new Replica(
                tempCountRequests > countOfNodes ? countOfNodes : tempCountRequests,
                countOfNodes
            ));
        } else {
            String[] split = replica.split("/");
            int requests = Integer.parseInt(split[0]);
            int nodes = Integer.parseInt(split[1]);
            if (requests > nodes || requests == 0) {
                throw new IllegalArgumentException("Count of nodes must be greater or equal then count of requests");
            }
            return Optional.of(new Replica(requests, nodes));
        }
    }

    public int getCountRequests() {
        return countRequests;
    }

    public int getCountOfNodes() {
        return countOfNodes;
    }
}
