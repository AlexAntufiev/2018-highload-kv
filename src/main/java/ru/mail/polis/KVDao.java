/*
 * Copyright 2018 (c) Vadim Tsesko <incubos@yandex.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.alexantufiev.entity.BytesEntity;

import java.io.Closeable;
import java.util.NoSuchElementException;

/**
 * Key-value DAO API
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
public interface KVDao extends Closeable {
    /**
     * Get a entity by {@code key} from storage.
     *
     * @param key key
     * @return found entity
     * @throws NoSuchElementException if entity not found
     */
    @NotNull
    byte[] get(@NotNull byte[] key) throws NoSuchElementException;

    /**
     * Insert {@code value} by {@code key} into storage.
     *
     * @param key   key
     * @param value value
     */
    void upsert(@NotNull byte[] key, @NotNull byte[] value);

    /**
     * Delete a entity by {@code key} from storage.
     *
     * @param key key
     */
    void remove(@NotNull byte[] key);

    BytesEntity getEntity(@NotNull byte[] key);

    boolean isAccessible();

    void isAccessible(boolean isAccessible);
}
