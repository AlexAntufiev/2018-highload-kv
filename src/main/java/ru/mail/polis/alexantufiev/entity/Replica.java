package ru.mail.polis.alexantufiev.entity;

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

    public Replica(String replica, int countOfNodes) throws NumberFormatException, IndexOutOfBoundsException {
        if (replica == null || replica.isEmpty()) {
            int tempCountRequests = countOfNodes / 2 + 1;
            countRequests = tempCountRequests > countOfNodes ? countOfNodes : tempCountRequests;
            this.countOfNodes = countOfNodes;
        } else {
            String[] split = replica.split("/");
            countRequests = Integer.parseInt(split[0]);
            this.countOfNodes = Integer.parseInt(split[1]);
        }
    }

    public int getCountRequests() {
        return countRequests;
    }

    public int getCountOfNodes() {
        return countOfNodes;
    }
}