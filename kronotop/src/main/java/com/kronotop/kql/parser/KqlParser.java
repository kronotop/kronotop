package com.kronotop.kql.parser;

import com.kronotop.kql.KqlNode;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.Document;

public class KqlParser {
    private final String query;

    KqlParser(String query) {
        this.query = query;
    }

    private void traverse(BsonReader reader) {
        switch (reader.getCurrentBsonType()) {
            case STRING:
                System.out.println("$$ STRING " + reader.readString());
                break;
            case INT32:
                System.out.println("$$ INT32 " + reader.readInt32());
                break;
            case ARRAY:
                readStartArray(reader);
                break;
            case DOCUMENT:
                readStartDocument(reader);
                break;
        }
    }

    private void readStartArray(BsonReader reader) {
        reader.readStartArray();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            traverse(reader);
        }
        reader.readEndArray();
    }

    private void readStartDocument(BsonReader reader) {
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            System.out.println(">> NAME >> " + reader.readName());
            traverse(reader);
        }
        reader.readEndDocument();
    }

    private KqlNode parse() {
        Document document = Document.parse(query);
        try (BsonReader reader = document.toBsonDocument().asBsonReader()) {
            readStartDocument(reader);
        }
        return new KqlNode();
    }

    public static KqlNode parse(String query) {
        return new KqlParser(query).parse();
    }
}
