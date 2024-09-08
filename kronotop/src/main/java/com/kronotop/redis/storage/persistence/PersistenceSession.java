package com.kronotop.redis.storage.persistence;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.Session;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PersistenceSession {
    private final Context context;
    private final RedisShard shard;
    private final ByteBuffer[] entries;
    private int size;
    private int cursor;

    public PersistenceSession(Context context, RedisShard shard, int capacity) {
        this.context = context;
        this.shard = shard;
        this.entries = new ByteBuffer[capacity];
    }

    /**
     * Packs the provided DataStructurePack into a ByteBuffer and stores it in the entries array.
     * The method increments the size based on the buffer's remaining bytes and returns the current cursor position.
     * The cursor is then incremented for the next pack operation.
     *
     * @param dataStructurePack the DataStructurePack to be packed and stored
     * @return the position in the entry array where the ByteBuffer was stored
     */
    public int pack(DataStructurePack dataStructurePack) {
        ByteBuffer buffer = dataStructurePack.pack();
        size += buffer.remaining();
        try {
            entries[cursor] = buffer;
            return cursor;
        } finally {
            cursor++;
        }
    }

    /**
     * Persists the data stored in the PersistenceSession to a storage system.
     * The data is packed into byte buffers and appended to the storage system.
     *
     * @throws IOException if an I/O error occurs while persisting the data
     */
    public Versionstamp[] persist() throws IOException {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Session session = new Session(tr);
            AppendResult result = shard.volume().append(session, entries);
            tr.commit().join();
            return result.getVersionstampedKeys();
        }
    }

    public int size() {
        return size;
    }

    public Context context() {
        return context;
    }

    public RedisShard shard() {
        return shard;
    }
}