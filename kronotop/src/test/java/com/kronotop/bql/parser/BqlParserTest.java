package com.kronotop.bql.parser;

import com.kronotop.bucket.bql.parser.BqlParser;
import org.junit.jupiter.api.Test;

import java.util.Random;

class BqlParserTest {
    @Test
    void test_parse() {
        //List<BqlOperator> result = BqlParser.parse("{ status: 'ALIVE', username: 'buraksezer', email: 'buraksezer@gmail.com', age: 36 }");
        //BqlParser.parse("{ status: {$eq: 'ALIVE'}, username: {$eq: 'buraksezer'} }");
        //List<BqlOperator> result = BqlParser.parse("{ tags: { $all: [ 'ssl' , 'security' ] } }");
        //List<BqlOperator> result = BqlParser.parse("{ quantity: { $nin: [ 5, 15 ] } }");

        Random rand = new Random();
        long total = 0;
        for (int i = 0; i< 100000; i++) {
            String query = String.format("{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: %d } } ], username: { $eq: 'buraksezer' }, tags: { $all: ['foo', 32]} }", rand.nextInt());
            long start = System.nanoTime();
            BqlParser.parse(query);
            long end = System.nanoTime();
            total += (end - start);
        }
        System.out.println(total/100000);
        //for (BqlOperator operator : result) {
        //    System.out.println(operator);
        //};
    }
}