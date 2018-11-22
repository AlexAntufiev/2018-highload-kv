package ru.mail.polis.alexantufiev.entity;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.CompoundByteIterable;

import java.time.LocalDateTime;

import static jetbrains.exodus.bindings.StringBinding.stringToEntry;

/**
 * TODO: add doc
 *
 * @author Aleksey Antufev
 * @version 1.1.0
 * @since 1.1.0 10.11.2018
 */
public class BytesEntity {

    private final byte[] bytes;
    private final LocalDateTime time;
    private Boolean isDeleted;

    public BytesEntity(byte[] bytes) {
        this.bytes = bytes;
        time = LocalDateTime.now();
        isDeleted = false;
    }

    public BytesEntity(ByteIterable metadata) {
        bytes = metadata.subIterable(0, metadata.getLength() - 26).getBytesUnsafe();
        time = LocalDateTime.parse(
            new String(metadata.subIterable(metadata.getLength() - 26, 23).getBytesUnsafe()));
        isDeleted = "T".equals(
            new String(metadata.subIterable(metadata.getLength() - 2, 1).getBytesUnsafe()));
    }

    public ByteIterable toByteIterable() {
        ByteIterable[] byteIterables = new ByteIterable[3];
        byteIterables[0] = new ArrayByteIterable(bytes);
        byteIterables[1] = stringToEntry(time.toString());
        byteIterables[2] = stringToEntry(isDeleted ? "T" : "F");
        return new CompoundByteIterable(byteIterables);
    }

    public void setDeleted(boolean deleted) {
        isDeleted = deleted;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public ByteIterable getByteIterableOfBytes() {
        return new ArrayByteIterable(bytes);
    }

    public LocalDateTime getTime() {
        return time;
    }

    public boolean isDeleted() {
        return isDeleted;
    }
}
