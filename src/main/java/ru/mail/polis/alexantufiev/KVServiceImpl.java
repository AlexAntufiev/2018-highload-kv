package ru.mail.polis.alexantufiev;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * The implementation of {@link KVService}.
 *
 * @author Aleksey Antufev
 * @version 1.0.0
 * @since 1.0.0 08.10.2018
 */
public class KVServiceImpl extends HttpServer implements KVService {
    @NotNull
    private final KVDao dao;

    public KVServiceImpl(int port, @NotNull KVDao dao) throws IOException {
        super(create(port));
        this.dao = dao;
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

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                tryExecuteWithBody(() -> dao.get(id.getBytes()), session, Response.OK);
                break;
            case Request.METHOD_PUT:
                tryExecute(() -> dao.upsert(id.getBytes(), request.getBody()), session, Response.CREATED);
                break;
            case Request.METHOD_DELETE:
                tryExecute(() -> dao.remove(id.getBytes()), session, Response.ACCEPTED);
                break;
            default:
                sendResponse(session, Response.METHOD_NOT_ALLOWED);
        }
    }

    private static void tryExecute(Runnable runnable, HttpSession session, String responseSuccessCode) {
        try {
            runnable.run();
            sendResponse(session, responseSuccessCode);
        } catch (NoSuchElementException exception) {
            sendResponse(session, Response.NOT_FOUND);
        } catch (Exception exception) {
            sendResponse(session, Response.INTERNAL_ERROR, exception.toString().getBytes());
        }
    }

    private static void tryExecuteWithBody(Supplier<byte[]> function, HttpSession session, String responseSuccessCode) {
        try {
            sendResponse(session, responseSuccessCode, function.get());
        } catch (NoSuchElementException exception) {
            sendResponse(session, Response.NOT_FOUND);
        } catch (Exception exception) {
            sendResponse(session, Response.INTERNAL_ERROR, exception.toString().getBytes());
        }
    }

    private static void sendResponse(
        @NotNull HttpSession session,
        @NotNull String statusCode,
        @NotNull byte[] empty
    ) {
        try {
            session.sendResponse(new Response(statusCode, empty));
        } catch (IOException e) {
            sendResponse(session, Response.INTERNAL_ERROR, e.toString().getBytes());
        }
    }

    private static void sendResponse(
        @NotNull HttpSession session,
        @NotNull String statusCode
    ) {
        sendResponse(session, statusCode, Response.EMPTY);
    }

    private static HttpServerConfig create(int port) {
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;

        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }
}
