package ru.mail.polis.alexantufiev.dao;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.CompoundByteIterable;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Environments;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.alexantufiev.entity.BytesEntity;

import java.io.File;
import java.time.LocalDateTime;
import java.util.NoSuchElementException;

import static jetbrains.exodus.bindings.StringBinding.stringToEntry;

/**
 * The implementation of {@link KVDao}.
 *
 * @author Aleksey Antufev
 * @version 1.1.0
 * @since 1.0.0 01.05.2018
 */
public class KVDaoImpl implements KVDao {

    private final Environment environment;
    //    private final PersistentEntityStore store;
    private static final String STORAGE_NAME = "MyStorage";
    private static final String propertyTime = "time";
    private static final String propertyIsDeleted = "isDeleted";
    private static final String propertyValue = "value";

    private boolean isAccessible;

    public KVDaoImpl(@NotNull File data) {
        environment = Environments.newInstance(data);
        isAccessible = true;
    }

    private ByteIterable bytesToEntry(@NotNull byte[] bytes) {
        return new ArrayByteIterable(bytes);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException {
        if (!isAccessible) {
            throw new NoAccessException();
        }
        BytesEntity entity = getEntity(key);
        if (entity.isDeleted()) {
            throw new NoSuchElementException("Entity was deleted");
        }
        return entity.getBytes();
    }

    @Override
    public BytesEntity getEntity(@NotNull byte[] key) {
        ByteIterable[] byteIterable = new ByteIterable[1];
        environment.executeInTransaction(txn -> byteIterable[0] = getStore(txn).get(txn, bytesToEntry(key)));
        if (byteIterable[0] == null) {
            throw new NoSuchElementException("Entity was not found");
        }
        return new BytesEntity(byteIterable[0]);
    }

    @Override
    public boolean isAccessible() {
        return isAccessible;
    }

    @Override
    public void isAccessible(boolean isAccessible) {
        this.isAccessible = isAccessible;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        if (!isAccessible) {
            throw new NoAccessException();
        }
        environment.executeInTransaction(txn -> {
            ByteIterable[] byteIterables = new ByteIterable[3];
            byteIterables[0] = bytesToEntry(value);
            byteIterables[1] = stringToEntry(LocalDateTime.now().toString());
            byteIterables[2] = stringToEntry("F");

            getStore(txn).put(txn, bytesToEntry(key), new CompoundByteIterable(byteIterables));
        });
    }

    public void upsert(@NotNull BytesEntity entity) {
        environment.executeInTransaction(txn -> getStore(txn).put(
            txn,
            bytesToEntry(entity.getBytes()),
            entity.toByteIterable()
        ));
    }

    @Override
    public void remove(@NotNull byte[] key) {
        if (!isAccessible) {
            throw new NoAccessException();
        }
        BytesEntity entity = getEntity(key);
        entity.setDeleted(true);
        environment.executeInTransaction(txn -> getStore(txn).put(txn, bytesToEntry(key), entity.toByteIterable()));
    }

    @NotNull
    private Store getStore(Transaction txn) {
        return environment.openStore(STORAGE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn);
    }

    @Override
    public void close() {
        environment.close();
        //        store.close();
    }
}
