/*
 * Copyright (c) 2023 Kronotop
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.redis;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * StringValueTest will test all functionalities of StringValue class, specifically the `decode` method.
 * The StringValue class represents a string value stored as a byte array,
 * and we can decode it from and encode it into a byte array using MessagePack serialization.
 * The decode method decodes the given byte array and returns a StringValue object.
 */
public class StringValueTest {

    /**
     * Method: decode(byte[] data)
     * When input data is given as StringValue encode with TTL then decode should create StringValue with same TTL and value.
     */
    @Test
    public void testDecodeWithTTL() throws IOException {
        StringValue expectedStringValue = new StringValue("Hello, World!".getBytes(StandardCharsets.UTF_8), 1000L);
        byte[] encodedData = expectedStringValue.encode();
        StringValue actualStringValue = StringValue.decode(encodedData);

        assertArrayEquals(expectedStringValue.getValue(), actualStringValue.getValue());
        assertEquals(expectedStringValue.getTTL(), actualStringValue.getTTL());
    }

    /**
     * Method: decode(byte[] data)
     * When input data is given as StringValue encode with no TTL then decode should create StringValue with same value and zero TTL.
     */
    @Test
    public void testDecodeNoTTL() throws IOException {
        StringValue expectedStringValue = new StringValue("Hello, World!".getBytes(StandardCharsets.UTF_8));
        byte[] encodedData = expectedStringValue.encode();
        StringValue actualStringValue = StringValue.decode(encodedData);

        assertArrayEquals(expectedStringValue.getValue(), actualStringValue.getValue());
        assertEquals(expectedStringValue.getTTL(), actualStringValue.getTTL());
    }

    /**
     * Method: decode(byte[] data)
     * When input data is zero length array then decode should throw IOException.
     */
    @Test
    public void testDecodeZeroLengthArray() throws IOException {
        byte[] encodedData = new byte[0];
        assertThrows(IOException.class, () -> StringValue.decode(encodedData));
    }

    /**
     * Test case for the encode method of StringValue class. In this test case,
     * we are checking that the method correctly encodes StringValue object
     * into byte array. We first construct a StringValue object, then encode it
     * to bytes using encode method, afterward we decode it into a new
     * StringValue object, and then we compare it to the original one.
     */
    @Test
    public void testEncode() throws IOException {
        byte[] value = "Hello World".getBytes(StandardCharsets.UTF_8);
        long ttl = 5000;
        StringValue stringValue = new StringValue(value, ttl);

        // Let's encode our string value into bytes
        byte[] encoded = stringValue.encode();

        // Now decode it back into a new StringValue object
        StringValue stringValueNew = StringValue.decode(encoded);

        // Assertions
        // The original and the new StringValue objects should be equal
        assertArrayEquals(stringValue.getValue(), stringValueNew.getValue());
        assertEquals(stringValue.getTTL(), stringValueNew.getTTL());
    }
}