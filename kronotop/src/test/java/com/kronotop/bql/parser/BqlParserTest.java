package com.kronotop.bql.parser;

import com.kronotop.bql.operators.BqlOperator;
import org.junit.jupiter.api.Test;

import java.util.List;

class BqlParserTest {
    @Test
    void test_parse() {
        //List<BqlOperator> result = BqlParser.parse("{ status: 'ALIVE', username: 'buraksezer', email: 'buraksezer@gmail.com', age: 36 }");
        //BqlParser.parse("{ status: {$eq: 'ALIVE'}, username: {$eq: 'buraksezer'} }");
        List<BqlOperator> result = BqlParser.parse("{ tags: { $all: [ 'ssl' , 'security' ] } }");
        //List<BqlOperator> result = BqlParser.parse("{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: 30 } } ], username: { $eq: 'buraksezer' }, tags: { $all: ['foo', 32]} }");
        for (BqlOperator operator : result) {
            System.out.println(operator);
        };
    }
}