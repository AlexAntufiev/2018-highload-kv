package ru.mail.polis.alexantufiev.service;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.net.URL;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The implementation of {@link KVService}.
 *
 * @author Aleksey Antufev
 * @version 1.1.0
 * @since 1.1.0 08.10.2018
 */
public abstract class DefaultKVService extends HttpServer implements KVService {
    @NotNull
    protected KVDao dao;

    protected Set<DefaultKVService> nodes;

    public DefaultKVService(int port) throws IOException {
        super(create(port));
    }

    protected DefaultKVService(@NotNull URL url, @NotNull KVDao dao) throws IOException {
        super(create(url));
        this.dao = dao;
    }

    protected static void tryExecute(Runnable runnable, HttpSession session, String responseSuccessCode) {
        try {
            runnable.run();
            sendResponse(session, responseSuccessCode);
        } catch (NoSuchElementException exception) {
            sendResponse(session, Response.NOT_FOUND);
        } catch (RuntimeException exception) {
            sendResponse(session, Response.INTERNAL_ERROR, exception.toString().getBytes());
        }
    }

    protected static void tryExecuteWithBody(
        Supplier<byte[]> function,
        HttpSession session,
        String responseSuccessCode
    ) {
        try {
            sendResponse(session, responseSuccessCode, function.get());
        } catch (NoSuchElementException exception) {
            sendResponse(session, Response.NOT_FOUND);
        } catch (RuntimeException exception) {
            sendResponse(session, Response.INTERNAL_ERROR, exception.toString().getBytes());
        }
    }

    protected static void sendResponse(
        @NotNull HttpSession session,
        @NotNull String statusCode,
        byte[] bytes
    ) {
        try {
            if (bytes == null) {
                sendResponse(session, Response.INTERNAL_ERROR, "Empty body".getBytes());
                return;
            }
            session.sendResponse(new Response(statusCode, bytes));
        } catch (IOException e) {
            sendResponse(session, Response.INTERNAL_ERROR, e.toString().getBytes());
        }
    }

    protected static void sendResponse(@NotNull HttpSession session, @NotNull String statusCode) {
        sendResponse(session, statusCode, Response.EMPTY);
    }

    protected static HttpServerConfig create(URL url) {
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = url.getPort();
        acceptorConfig.address = url.getHost();

        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }

    protected static HttpServerConfig create(int port) {
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;

        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }
}