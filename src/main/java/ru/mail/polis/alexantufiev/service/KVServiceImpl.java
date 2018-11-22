package ru.mail.polis.alexantufiev.service;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import ru.mail.polis.alexantufiev.dao.NoAccessException;
import ru.mail.polis.alexantufiev.entity.BytesEntity;
import ru.mail.polis.alexantufiev.entity.Replica;
import ru.mail.polis.alexantufiev.entity.State;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The implementation of {@link HttpServer}.
 *
 * @author Aleksey Antufev
 * @version 1.1.0
 * @since 1.0.0 08.10.2018
 */
public class KVServiceImpl extends HttpServer implements KVService {

    private static final String PATH = "/v0/entity";
    private static final String PATH_WITH_ID_PATTERN = "/v0/entity?id=%s";
    private static final String NO_REPLICA = "NO_REPLICA: true";

    @NotNull
    private final KVDao dao;

    private Map<String, HttpClient> nodes;

    public KVServiceImpl(int port, KVDao dao) throws IOException {
        super(create(port));
        this.dao = dao;
    }

    public KVServiceImpl(int port, @NotNull Set<String> topology, @NotNull KVDao dao) throws IOException {
        this(port, dao);
        nodes = new HashMap<>(topology.size());
        for (String node : topology) {
            if (port != new URL(node).getPort()) {
                nodes.put(node, new HttpClient(new ConnectionString(node)));
            } else {
                nodes.put(node, null);
            }
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        dao.isAccessible(true);
    }

    @Override
    public synchronized void stop() {
        super.stop();
        dao.isAccessible(false);
    }

    @Path("/v0/status")
    public Response getStatus() throws PoolException {
        if (dao.isAccessible()) {
            return Response.ok("");
        } else {
            throw new PoolException();
        }
    }

    @Path(PATH)
    public void handleDefault(Request request, HttpSession session) {
        if (!PATH.equals(request.getPath())) {
            sendError(session, Response.BAD_REQUEST);
            return;
        }

        String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            sendError(session, Response.BAD_REQUEST);
            return;
        }

        Optional<Replica> optionalReplica;
        try {
            optionalReplica = Replica.of(
                request.getParameter("replicas="),
                nodes.size(),
                request.getHeader(NO_REPLICA) != null
            );
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            sendError(session, Response.BAD_REQUEST);
            return;
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                getEntity(session, id, optionalReplica);
                break;
            case Request.METHOD_PUT:
                putEntity(request, session, id, optionalReplica);
                break;
            case Request.METHOD_DELETE:
                deleteEntity(session, id, optionalReplica);
                break;
            default:
                sendError(session, Response.METHOD_NOT_ALLOWED);
        }
    }

    private void getEntity(HttpSession session, String id, Optional<Replica> replica) {
        if (!replica.isPresent()) {
            executeAndSendResponse(() -> dao.get(id.getBytes()), session, Response.OK);
            return;
        }
        int countOfExpectedNodes = replica.get().getCountOfNodes();
        byte[] bytes = null;
        List<State> states = new ArrayList<>(nodes.size());
        for (Map.Entry<String, HttpClient> entry : nodes.entrySet()) {
            if (countOfExpectedNodes > 0) {
                try {
                    HttpClient value = entry.getValue();
                    if (value == null) {
                        BytesEntity entity = dao.getEntity(id.getBytes());
                        if (entity.isDeleted()) {
                            states.add(State.DELETED);
                        } else {
                            states.add(State.EXIST);
                            bytes = entity.getBytes();
                        }
                    } else {
                        Response response = value.get(String.format(PATH_WITH_ID_PATTERN, id), NO_REPLICA);
                        switch (response.getStatus()) {
                            case 200:
                                states.add(State.EXIST);
                                bytes = response.getBody();
                                break;
                            case 404:
                                states.add(State.NO_EXIST);
                                break;
                            case 504:
                                states.add(State.DELETED);
                                break;
                            default:
                                break;
                        }
                    }
                } catch (NoSuchElementException e) {
                    states.add(State.NO_EXIST);
                } catch (IOException | HttpException | InterruptedException | NoAccessException | PoolException e) {
                    e.printStackTrace();
                }
            }
            countOfExpectedNodes--;
        }
        int countOfRespondedNodes = 0;
        if (states.size() < replica.get().getCountRequests()) {
            sendError(session, Response.GATEWAY_TIMEOUT);
        }
        for (State state : states) {
            if (state == State.DELETED) {
                sendError(session, Response.NOT_FOUND);
                return;
            }
            if (state == State.EXIST) {
                countOfRespondedNodes
                    ++;
            }
        }
        if (states.contains(State.NO_EXIST)) {
            if (countOfRespondedNodes
                == replica.get().getCountRequests()) {
                sendResponse(session, Response.OK, bytes);
            } else {
                sendError(session, Response.NOT_FOUND);
            }
        }
        sendResponse(session, Response.OK, bytes);
    }

    private void putEntity(Request request, HttpSession session, String id, Optional<Replica> replica) {
        if (!replica.isPresent()) {
            executeAndSendResponse(() -> dao.upsert(id.getBytes(), request.getBody()), session, Response.CREATED);
            return;
        }
        int countOfRespondedNodes = 0;
        int countOfExpectedNodes = replica.get().getCountOfNodes();
        for (Map.Entry<String, HttpClient> entry : nodes.entrySet()) {
            if (countOfExpectedNodes > 0) {
                try {
                    HttpClient value = entry.getValue();
                    if (value == null) {
                        dao.upsert(id.getBytes(), request.getBody());
                        countOfRespondedNodes++;
                    } else {
                        Response response = value.put(
                            String.format(PATH_WITH_ID_PATTERN, id),
                            request.getBody(),
                            NO_REPLICA
                        );
                        switch (response.getStatus()) {
                            case 201:
                                countOfRespondedNodes++;
                                break;
                            default:
                                break;
                        }
                    }
                } catch (IOException | InterruptedException | HttpException | PoolException | NoAccessException e) {
                    e.printStackTrace();
                }
            }
            countOfExpectedNodes--;
        }
        if (countOfRespondedNodes >= replica.get().getCountRequests()) {
            sendResponse(session, Response.CREATED);
        } else {
            sendError(session, Response.GATEWAY_TIMEOUT);
        }
    }

    private void deleteEntity(HttpSession session, String id, Optional<Replica> replica) {
        if (!replica.isPresent()) {
            executeAndSendResponse(() -> dao.remove(id.getBytes()), session, Response.ACCEPTED);
            return;
        }
        int countOfRespondedNodes = 0;
        int countOfExpectedNodes = replica.get().getCountOfNodes();
        for (Map.Entry<String, HttpClient> entry : nodes.entrySet()) {
            if (countOfExpectedNodes > 0) {
                try {
                    HttpClient value = entry.getValue();
                    if (value == null) {
                        dao.remove(id.getBytes());
                        countOfRespondedNodes++;
                    } else {
                        Response response = value.delete(String.format(PATH_WITH_ID_PATTERN, id), NO_REPLICA);
                        switch (response.getStatus()) {
                            case 202:
                                countOfRespondedNodes++;
                                break;
                            default:
                                break;
                        }
                    }
                } catch (HttpException | IOException | InterruptedException | NoAccessException | PoolException e) {
                    e.printStackTrace();
                } catch (NoSuchElementException e) {
                    countOfRespondedNodes++;
                }
            }
            countOfExpectedNodes--;
        }

        if (countOfRespondedNodes >= replica.get().getCountRequests()) {
            sendResponse(session, Response.ACCEPTED);
        } else {
            sendError(session, Response.GATEWAY_TIMEOUT);
        }
    }

    protected static void executeAndSendResponse(Runnable runnable, HttpSession session, String responseCode) {
        try {
            runnable.run();
            sendResponse(session, new Response(responseCode, Response.EMPTY));
        } catch (NoSuchElementException exception) {
            sendError(session, Response.NOT_FOUND);
        } catch (RuntimeException exception) {
            exception.printStackTrace();
        }
    }

    protected static void executeAndSendResponse(Supplier<byte[]> runnable, HttpSession session, String responseCode) {
        try {
            sendResponse(session, new Response(responseCode, runnable.get()));
        } catch (NoSuchElementException exception) {
            sendError(session, Response.NOT_FOUND);
        } catch (RuntimeException exception) {
            exception.printStackTrace();
        }
    }

    protected static void sendResponse(@NotNull HttpSession session, Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected static void sendResponse(@NotNull HttpSession session, String status) {
        sendResponse(session, status, Response.EMPTY);
    }

    protected static void sendResponse(@NotNull HttpSession session, String status, byte[] bytes) {
        try {
            session.sendResponse(new Response(status, bytes));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected static void sendError(@NotNull HttpSession session, String status, String message) {
        try {
            session.sendError(status, message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected static void sendError(@NotNull HttpSession session, String status) {
        sendError(session, status, "");
    }

    protected static HttpServerConfig create(int port) {
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;

        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }
}
