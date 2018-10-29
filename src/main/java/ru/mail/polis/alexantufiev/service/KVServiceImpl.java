package ru.mail.polis.alexantufiev.service;

import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.alexantufiev.entity.Replica;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The implementation of {@link DefaultKVService}.
 *
 * @author Aleksey Antufev
 * @version 1.1.0
 * @since 1.0.0 08.10.2018
 */
public class KVServiceImpl extends DefaultKVService {

    public KVServiceImpl(int port) throws IOException {
        super(port);
    }

    private KVServiceImpl(@NotNull URL url, @NotNull KVDao dao) throws IOException {
        super(url, dao);
    }

    public KVServiceImpl(int port, @NotNull Set<String> topology, @NotNull KVDao dao) throws IOException {
        this(port);
        nodes = new HashSet<>(topology.size());
        for (String node : topology) {
            nodes.add(new KVServiceImpl(new URL(node), dao));
        }
    }

    private KVServiceImpl createNode(String node) throws IOException {
        return new KVServiceImpl(new URL(node), dao);
    }

    @Path("/v0/status")
    public Response getStatus() {
        return Response.ok("I'm ok");
    }

    @Override
    public void handleDefault(Request request, HttpSession session) {
        if (!"/v0/entity".equals(request.getPath())) {
            sendResponse(session, Response.BAD_REQUEST);
            return;
        }

        String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            sendResponse(session, Response.BAD_REQUEST);
            return;
        }

        Replica replica;
        try {
            replica = new Replica(request.getParameter("replicas="), nodes.size());
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            sendResponse(session, Response.BAD_REQUEST);
            return;
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                getEntity(session, id, replica);
                break;
            case Request.METHOD_PUT:
                putEntity(request, session, id, replica);
                break;
            case Request.METHOD_DELETE:
                deleteEntity(session, id, replica);
                break;
            default:
                sendResponse(session, Response.METHOD_NOT_ALLOWED);
        }
    }

    private void putEntity(Request request, HttpSession session, String id, Replica replica) {
        int countOfRespondedNodes = 0;
        int countRequests = replica.getCountRequests();
        int countOfExpectedNodes = replica.getCountOfNodes();
        for (DefaultKVService node : nodes) {
            if (countRequests > 0) {
                try {
                    node.dao.upsert(id.getBytes(), request.getBody());
                    countOfRespondedNodes++;
                } catch (NoSuchElementException exception) {
                    //System.out.println(String.format("ERROR: %s", exception.getMessage()));
                    sendResponse(session, Response.NOT_FOUND);
                    return;
                }
                countRequests--;
            }
        }
        if (countOfRespondedNodes >= countOfExpectedNodes) {
            sendResponse(session, Response.CREATED);
            return;
        }
        sendResponse(session, "504 Not Enough Replicas");
    }

    private void getEntity(HttpSession session, String id, Replica replica) {
        int countOfRespondedNodes = 0;
        int countRequests = replica.getCountRequests();
        int countOfExpectedNodes = replica.getCountOfNodes();
        byte[] bytes = null;
        for (DefaultKVService node : nodes) {
            if (countRequests > 0) {
                try {
                    bytes = node.dao.get(id.getBytes());
                    countOfRespondedNodes++;
                } catch (NoSuchElementException exception) {
                    //                    System.out.println(String.format("ERROR: %s", exception.getMessage()));
                    sendResponse(session, Response.NOT_FOUND);
                    return;
                }
                countRequests--;
            }
        }
        if (countOfRespondedNodes >= countOfExpectedNodes) {
            sendResponse(session, Response.OK, bytes);
            return;
        }
        sendResponse(session, "504 Not Enough Replicas");
    }

    private void deleteEntity(HttpSession session, String id, Replica replica) {
        int countOfRespondedNodes = 0;
        int countRequests = replica.getCountRequests();
        int countOfExpectedNodes = replica.getCountOfNodes();
        for (DefaultKVService node : nodes) {
            if (countRequests > 0) {
                try {
                    node.dao.remove(id.getBytes());
                    countOfRespondedNodes++;
                } catch (NoSuchElementException exception) {
                    //                    System.out.println(String.format("ERROR: %s", exception.getMessage()));
                    sendResponse(session, Response.NOT_FOUND);
                    return;
                }
                countRequests--;
            }
        }
        if (countOfRespondedNodes >= countOfExpectedNodes) {
            sendResponse(session, Response.ACCEPTED);
            return;
        }
        sendResponse(session, "504 Not Enough Replicas");
    }
}