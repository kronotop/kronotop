package com.kronotop.volume;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class EntryMetadataTest {
    @Test
    void encode_then_decode() {
        EntryMetadata entryMetadata = new EntryMetadata("0000000000000000000", 10, 5);
        EntryMetadata decodedEntryMetadata = EntryMetadata.decode(ByteBuffer.wrap(entryMetadata.encode().array()));
        System.out.println(decodedEntryMetadata);
    }
}