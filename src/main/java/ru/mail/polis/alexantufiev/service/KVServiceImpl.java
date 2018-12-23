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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import ru.mail.polis.alexantufiev.dao.NoAccessException;
import ru.mail.polis.alexantufiev.entity.BytesEntity;
import ru.mail.polis.alexantufiev.entity.Replica;
import ru.mail.polis.alexantufiev.entity.State;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
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
    private static final Logger logger = LoggerFactory.getLogger(KVServiceImpl.class);

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
            if (port == new URL(node).getPort()) {
                nodes.put(node, null);
            } else {
                nodes.put(node, new HttpClient(new ConnectionString(node)));
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
        logger.debug(String.format(
            "*** HANDLE REQUEST *** METHOD='%s'\n PATH='%s'\n QUERY='%s'\n HEADERS='%s'\n BODY='%s'\n ",
            request.getMethod(),
            request.getPath(),
            request.getQueryString(),
            Arrays.toString(request.getHeaders()),
            Arrays.toString(request.getBody())
        ));
        String path = request.getPath();
        if (!PATH.equals(path)) {
            logger.error(String.format("BAD PATH: %s", path));
            sendError(session, Response.BAD_REQUEST);
            return;
        }

        String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            logger.error(String.format("BAD ID: %s", id));
            sendError(session, Response.BAD_REQUEST);
            return;
        }

        Optional<Replica> optionalReplica;
        String replicas = request.getParameter("replicas=");
        try {
            optionalReplica = Replica.of(
                replicas,
                nodes.size(),
                request.getHeader(NO_REPLICA) != null
            );
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            logger.error(String.format("BAD REPLICA: %s", replicas));
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
            logger.debug(String.format("*** RECEIVE REQUEST : GET IN LOCAL DAO *** ID='%s'", id));
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
                        boolean deleted = entity.isDeleted();
                        logger.debug(String.format("*** SEND REQUEST : GET IN DAO *** STATUS_DELETED='%s' ID='%s'", deleted, id));
                        if (deleted) {
                            states.add(State.DELETED);
                        } else {
                            states.add(State.EXIST);
                            bytes = entity.getBytes();
                        }
                    } else {
                        logger.debug(String.format("*** SEND REQUEST : GET IN DAO *** ID='%s'", id));
                        Response response = value.get(String.format(PATH_WITH_ID_PATTERN, id), NO_REPLICA);
                        int status = response.getStatus();
                        switch (status) {
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
                                logger.error(String.format("WRONG STATUS: %d", status));
                                break;
                        }
                    }
                } catch (NoSuchElementException e) {
                    states.add(State.NO_EXIST);
                } catch (IOException | HttpException | InterruptedException | NoAccessException | PoolException e) {
                    logger.error("catch exception in get method", e);
                }
            }
            countOfExpectedNodes--;
        }
        int countOfRespondedNodes = 0;
        if (states.size() < replica.get().getCountRequests()) {
            logger.debug(String.format("*** SEND FINAL RESPONSE *** STATUS='%s'", Response.GATEWAY_TIMEOUT));
            sendError(session, Response.GATEWAY_TIMEOUT);
        }
        for (State state : states) {
            if (state == State.DELETED) {
                logger.debug(String.format("*** SEND FINAL RESPONSE *** STATUS='%s'", Response.NOT_FOUND));
                sendError(session, Response.NOT_FOUND);
                return;
            }
            if (state == State.EXIST) {
                countOfRespondedNodes++;
            }
        }
        if (states.contains(State.NO_EXIST)) {
            if (countOfRespondedNodes == replica.get().getCountRequests()) {
                logger.debug(String.format("*** SEND FINAL RESPONSE *** STATUS='%s'", Response.OK));
                sendResponse(session, Response.OK, bytes);
            } else {
                logger.debug(String.format("*** SEND FINAL RESPONSE *** STATUS='%s'", Response.NOT_FOUND));
                sendError(session, Response.NOT_FOUND);
            }
        }
        logger.debug(String.format("*** SEND FINAL RESPONSE *** STATUS='%s'", Response.OK));
        sendResponse(session, Response.OK, bytes);
    }

    private void putEntity(Request request, HttpSession session, String id, Optional<Replica> replica) {
        if (!replica.isPresent()) {
                logger.debug(String.format("*** RECEIVE REQUEST : INSERT INTO LOCAL DAO *** ID='%s'", id));
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
                            logger.debug(String.format("*** INSERT INTO LOCAL DAO *** ID='%s'", id));
                            dao.upsert(id.getBytes(), request.getBody());
                            countOfRespondedNodes++;
                    } else {
                            logger.debug(String.format("*** SEND REQUEST : INSERT DAO *** ID='%s'", id));
                            Response response = value.put(
                                String.format(PATH_WITH_ID_PATTERN, id),
                                request.getBody(),
                                NO_REPLICA
                            );
                            int status = response.getStatus();
                            if (status == 201) {
                                countOfRespondedNodes++;
                            } else {
                                logger.error(String.format("WRONG STATUS: %d", status));
                            }
                        }
                } catch (IOException | InterruptedException | HttpException | PoolException | NoAccessException e) {
                    logger.error("catch exception in PUT method", e);
                }
            }
            countOfExpectedNodes--;
        }
        if (countOfRespondedNodes >= replica.get().getCountRequests()) {
            logger.debug(String.format("*** SEND FINAL RESPONSE *** STATUS='%s'", Response.CREATED));
            sendResponse(session, Response.CREATED);
        } else {
            logger.debug(String.format("*** SEND FINAl RESPONSE *** STATUS='%s'", Response.GATEWAY_TIMEOUT));
            sendError(session, Response.GATEWAY_TIMEOUT);
        }
    }

    private void deleteEntity(HttpSession session, String id, Optional<Replica> replica) {
        if (!replica.isPresent()) {
            logger.debug(String.format("*** RECEIVE REQUEST : DELETE IN LOCAL DAO *** ID='%s'", id));
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
                        logger.debug(String.format("*** DELETE IN LOCAL DAO *** ID='%s'", id));
                        dao.remove(id.getBytes());
                        countOfRespondedNodes++;
                    } else {
                        logger.debug(String.format("*** SEND REQUEST : DELETE IN DAO *** ID='%s'", id));
                        Response response = value.delete(String.format(PATH_WITH_ID_PATTERN, id), NO_REPLICA);
                        int status = response.getStatus();
                        if (status == 202) {
                            countOfRespondedNodes++;
                        } else {
                            logger.error(String.format("WRONG STATUS: %d", status));
                        }
                    }
                } catch (HttpException | IOException | InterruptedException | NoAccessException | PoolException e) {
                    logger.error("catch exception in DELETE method", e);
                } catch (NoSuchElementException e) {
                    countOfRespondedNodes++;
                }
            }
            countOfExpectedNodes--;
        }

        if (countOfRespondedNodes >= replica.get().getCountRequests()) {
            logger.debug(String.format("*** SEND FINAl RESPONSE *** STATUS='%s'", Response.ACCEPTED));
            sendResponse(session, Response.ACCEPTED);
        } else {
            logger.debug(String.format("*** SEND FINAl RESPONSE *** STATUS='%s'", Response.GATEWAY_TIMEOUT));
            sendError(session, Response.GATEWAY_TIMEOUT);
        }
    }

    protected static void executeAndSendResponse(Runnable runnable, HttpSession session, String status) {
        try {
            runnable.run();
            sendResponse(session, new Response(status, Response.EMPTY));
        } catch (NoSuchElementException exception) {
            sendError(session, Response.NOT_FOUND);
        } catch (RuntimeException exception) {
            logger.debug(String.format("*** FAILED EXECUTE AND SEND RESPONSE *** STATUS='%s' MESSAGE='%s'", status,
                "EMPTY"
            ), exception);
        }
    }

    protected static void executeAndSendResponse(Supplier<byte[]> runnable, HttpSession session, String status) {
        try {
            sendResponse(session, new Response(status, runnable.get()));
        } catch (NoSuchElementException exception) {
            sendError(session, Response.NOT_FOUND);
        } catch (RuntimeException exception) {
            logger.debug(String.format("*** FAILED EXECUTE AND SEND RESPONSE *** STATUS='%s' MESSAGE='%s'", status,
                "EMPTY"
            ), exception);
        }
    }

    protected static void sendResponse(@NotNull HttpSession session, Response response) {
        try {
            logger.debug(String.format("*** SEND RESPONSE *** STATUS='%s' BYTES='%s'", response.getStatus(), Arrays.toString(response.getBody())));
            session.sendResponse(response);
        } catch (IOException e) {
            logger.debug(String.format("*** FAILED SEND RESPONSE *** STATUS='%s' MESSAGE='%s'", response.getStatus(),
                Arrays.toString(response.getBody())
            ), e);
        }
    }

    protected static void sendResponse(@NotNull HttpSession session, String status) {
        sendResponse(session, status, Response.EMPTY);
    }

    protected static void sendResponse(@NotNull HttpSession session, String status, byte[] bytes) {
        sendResponse(session, new Response(status, bytes));
    }

    protected static void sendError(@NotNull HttpSession session, String status, String message) {
        try {
            logger.debug(String.format("*** SEND ERROR *** STATUS='%s' MESSAGE='%s'", status, message));
            session.sendError(status, message);
        } catch (IOException e) {
            logger.debug(String.format("*** FAILED SEND ERROR *** STATUS='%s' MESSAGE='%s'", status, message), e);
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
