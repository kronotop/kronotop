// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner;

public class TestQuery {
    public static final String OR_FILTER_WITH_TWO_SUB_FILTERS = "{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: 30 } } ] }";

    public static final String SINGLE_FIELD_WITH_STRING_TYPE_AND_GTE = "{ a: { $gte: 'string-value' } }";

    public static final String SINGLE_FIELD_WITH_IN32_TYPE_AND_EQ = "{ a: { $eq: 20 } }";

    public static final String IMPLICIT_AND_FILTER = "{ status: 'ALIVE', username: 'kronotop-admin' }";

    public static final String NO_CHILD_EXPRESSION = "{ }";

    public static final String IMPLICIT_EQ_FILTER = "{ status: 'ALIVE' }";

    public static final String EXPLICIT_EQ_FILTER = "{ status: { $eq: 'ALIVE' } }";

    public static final String EXPLICIT_EQ_FILTER_WITH_IMPLICIT_AND_FILTER = "{ status: { $eq: 'ALIVE' }, qty: { $lt: 30 } }";

    public static final String EXPLICIT_AND_FILTER_WITH_TWO_SUB_FILTERS = "{ $and: [ { status: {$eq: 'A' } }, { qty: { $lt: 30 } } ] }";

    public static final String NOT_EQUALS_FILTER_WITH_IMPLICIT_EQ_FILTER = "{ status: { $ne: 'A' } }";

    public static final String NOT_EQUALS_FILTER_WITH_EXPLICIT_EQ_FILTER = "{ status: { $ne: { $eq: 'A' } } }";

    public static final String EXISTS_FILTER = "{ price: { $exists: true } }";

    public static final String IMPLICIT_AND_WITH_NE_AND_EXISTS = "{ price: { $ne: 1.99, $exists: true } }";

    public static final String COMPLEX_QUERY_ONE = "{ $and: [{ $or: [ { qty: { $lt : 10 } }, { qty : { $gt: 50 } } ] },{ $or: [ { sale: true }, { price : { $lt : 5 } } ] }]}";
}
