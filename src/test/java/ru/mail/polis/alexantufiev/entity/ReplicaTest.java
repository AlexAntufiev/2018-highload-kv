package ru.mail.polis.alexantufiev.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link Replica}.
 *
 * @author Aleksey Antufev
 * @version 1.1.0
 * @since 1.1.0 16.10.2018
 */
class ReplicaTest {

    @Test
    void getCountRequests() {
        Replica replica = new Replica("1/2", 2);

        assertEquals(1, replica.getCountRequests(), "Replicas count of requests must be set up.");
        assertEquals(2, replica.getCountOfNodes(), "Replicas count of nodes must be set up.");
    }

    @Test
    void expectNumberFormatException() {
        Assertions.assertThrows(
            NumberFormatException.class, () -> new Replica("1/a", 1), "PatternSyntaxException must be thrown.");
    }

    @Test
    void expectIndexOutOfBoundsException() {
        Assertions.assertThrows(
            IndexOutOfBoundsException.class, () -> new Replica("12", 1), "PatternSyntaxException must be thrown.");
    }

    @Test
    void getEmptyReplicaNode() {
        int countOfNodes = 1;
        Replica replica = new Replica("", countOfNodes);

        assertEquals(1, replica.getCountRequests(), "Replicas count of requests must be set up.");
        assertEquals(countOfNodes, replica.getCountOfNodes(), "Replicas count of nodes must be set up.");
    }

    @Test
    void getDefaultReplica1Node() {
        int countOfNodes = 1;
        Replica replica = new Replica(null, countOfNodes);

        assertEquals(1, replica.getCountRequests(), "Replicas count of requests must be set up.");
        assertEquals(countOfNodes, replica.getCountOfNodes(), "Replicas count of nodes must be set up.");
    }

    @Test
    void getDefaultReplica2Nodes() {
        int countOfNodes = 2;
        Replica replica = new Replica(null, countOfNodes);

        assertEquals(2, replica.getCountRequests(), "Replicas count of requests must be set up.");
        assertEquals(countOfNodes, replica.getCountOfNodes(), "Replicas count of nodes must be set up.");
    }

    @Test
    void getDefaultReplica3Nodes() {
        int countOfNodes = 3;
        Replica replica = new Replica(null, countOfNodes);

        assertEquals(2, replica.getCountRequests(), "Replicas count of requests must be set up.");
        assertEquals(countOfNodes, replica.getCountOfNodes(), "Replicas count of nodes must be set up.");
    }

    @Test
    void getDefaultReplica4Nodes() {
        int countOfNodes = 4;
        Replica replica = new Replica(null, countOfNodes);

        assertEquals(3, replica.getCountRequests(), "Replicas count of requests must be set up.");
        assertEquals(countOfNodes, replica.getCountOfNodes(), "Replicas count of nodes must be set up.");
    }

    @Test
    void getDefaultReplica5Nodes() {
        int countOfNodes = 5;
        Replica replica = new Replica(null, countOfNodes);

        assertEquals(3, replica.getCountRequests(), "Replicas count of requests must be set up.");
        assertEquals(countOfNodes, replica.getCountOfNodes(), "Replicas count of nodes must be set up.");
    }
}