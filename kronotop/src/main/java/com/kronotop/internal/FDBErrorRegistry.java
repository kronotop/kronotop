/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.internal;

import java.util.HashMap;
import java.util.Map;

/*
From the original FDB documents:

FoundationDB may return the following error codes from API functions. If you need to check for specific errors
(for example, to implement custom retry logic), you must use the numerical code, since the other fields are particularly
likely to change unexpectedly. Error handling logic should also be prepared for new error codes which are not listed here.

 */

/**
 * FoundationDB error codes mapping.
 *
 * @see <a href="https://apple.github.io/foundationdb/api-error-codes.html">FoundationDB API Error Codes</a>
 */
public class FDBErrorRegistry {

    private static final Map<Integer, FDBErrorInfo> ERROR_CODES;

    static {
        ERROR_CODES = new HashMap<>();
        ERROR_CODES.put(0, new FDBErrorInfo("SUCCESS", "Success"));
        ERROR_CODES.put(1000, new FDBErrorInfo("OPERATION_FAILED", "Operation failed"));
        ERROR_CODES.put(1004, new FDBErrorInfo("TIMED_OUT", "Operation timed out"));
        ERROR_CODES.put(1007, new FDBErrorInfo("TRANSACTION_TOO_OLD", "Transaction is too old to perform reads or be committed"));
        ERROR_CODES.put(1009, new FDBErrorInfo("FUTURE_VERSION", "Request for future version"));
        ERROR_CODES.put(1020, new FDBErrorInfo("NOT_COMMITTED", "Transaction not committed due to conflict with another transaction"));
        ERROR_CODES.put(1021, new FDBErrorInfo("COMMIT_UNKNOWN_RESULT", "Transaction may or may not have committed"));
        ERROR_CODES.put(1025, new FDBErrorInfo("TRANSACTION_CANCELLED", "Operation aborted because the transaction was cancelled"));
        ERROR_CODES.put(1031, new FDBErrorInfo("TRANSACTION_TIMED_OUT", "Operation aborted because the transaction timed out"));
        ERROR_CODES.put(1032, new FDBErrorInfo("TOO_MANY_WATCHES", "Too many watches currently set"));
        ERROR_CODES.put(1034, new FDBErrorInfo("WATCHES_DISABLED", "Watches cannot be set if read your writes is disabled"));
        ERROR_CODES.put(1036, new FDBErrorInfo("ACCESSED_UNREADABLE", "Read or wrote an unreadable key"));
        ERROR_CODES.put(1037, new FDBErrorInfo("PROCESS_BEHIND", "Storage process does not have recent mutations"));
        ERROR_CODES.put(1038, new FDBErrorInfo("DATABASE_LOCKED", "Database is locked"));
        ERROR_CODES.put(1039, new FDBErrorInfo("CLUSTER_VERSION_CHANGED", "Cluster has been upgraded to a new protocol version"));
        ERROR_CODES.put(1040, new FDBErrorInfo("EXTERNAL_CLIENT_ALREADY_LOADED", "External client has already been loaded"));
        ERROR_CODES.put(1042, new FDBErrorInfo("PROXY_MEMORY_LIMIT_EXCEEDED", "CommitProxy commit memory limit exceeded"));
        ERROR_CODES.put(1051, new FDBErrorInfo("BATCH_TRANSACTION_THROTTLED", "Batch GRV request rate limit exceeded"));
        ERROR_CODES.put(1101, new FDBErrorInfo("OPERATION_CANCELLED", "Asynchronous operation cancelled"));
        ERROR_CODES.put(1102, new FDBErrorInfo("FUTURE_RELEASED", "Future has been released"));
        ERROR_CODES.put(1213, new FDBErrorInfo("TAG_THROTTLED", "Transaction tag is being throttled"));
        ERROR_CODES.put(1500, new FDBErrorInfo("PLATFORM_ERROR", "Platform error"));
        ERROR_CODES.put(1501, new FDBErrorInfo("LARGE_ALLOC_FAILED", "Large block allocation failed"));
        ERROR_CODES.put(1502, new FDBErrorInfo("PERFORMANCE_COUNTER_ERROR", "QueryPerformanceCounter error"));
        ERROR_CODES.put(1510, new FDBErrorInfo("IO_ERROR", "Disk i/o operation failed"));
        ERROR_CODES.put(1511, new FDBErrorInfo("FILE_NOT_FOUND", "File not found"));
        ERROR_CODES.put(1512, new FDBErrorInfo("BIND_FAILED", "Unable to bind to network"));
        ERROR_CODES.put(1513, new FDBErrorInfo("FILE_NOT_READABLE", "File could not be read"));
        ERROR_CODES.put(1514, new FDBErrorInfo("FILE_NOT_WRITABLE", "File could not be written"));
        ERROR_CODES.put(1515, new FDBErrorInfo("NO_CLUSTER_FILE_FOUND", "No cluster file found in current directory or default location"));
        ERROR_CODES.put(1516, new FDBErrorInfo("FILE_TOO_LARGE", "File too large to be read"));
        ERROR_CODES.put(2000, new FDBErrorInfo("CLIENT_INVALID_OPERATION", "Invalid API call"));
        ERROR_CODES.put(2002, new FDBErrorInfo("COMMIT_READ_INCOMPLETE", "Commit with incomplete read"));
        ERROR_CODES.put(2003, new FDBErrorInfo("TEST_SPECIFICATION_INVALID", "Invalid test specification"));
        ERROR_CODES.put(2004, new FDBErrorInfo("KEY_OUTSIDE_LEGAL_RANGE", "Key outside legal range"));
        ERROR_CODES.put(2005, new FDBErrorInfo("INVERTED_RANGE", "Range begin key larger than end key"));
        ERROR_CODES.put(2006, new FDBErrorInfo("INVALID_OPTION_VALUE", "Option set with an invalid value"));
        ERROR_CODES.put(2007, new FDBErrorInfo("INVALID_OPTION", "Option not valid in this context"));
        ERROR_CODES.put(2008, new FDBErrorInfo("NETWORK_NOT_SETUP", "Action not possible before the network is configured"));
        ERROR_CODES.put(2009, new FDBErrorInfo("NETWORK_ALREADY_SETUP", "Network can be configured only once"));
        ERROR_CODES.put(2010, new FDBErrorInfo("READ_VERSION_ALREADY_SET", "Transaction already has a read version set"));
        ERROR_CODES.put(2011, new FDBErrorInfo("VERSION_INVALID", "Version not valid"));
        ERROR_CODES.put(2012, new FDBErrorInfo("RANGE_LIMITS_INVALID", "Range limits not valid"));
        ERROR_CODES.put(2013, new FDBErrorInfo("INVALID_DATABASE_NAME", "Database name must be 'DB'"));
        ERROR_CODES.put(2014, new FDBErrorInfo("ATTRIBUTE_NOT_FOUND", "Attribute not found in string"));
        ERROR_CODES.put(2015, new FDBErrorInfo("FUTURE_NOT_SET", "Future not ready"));
        ERROR_CODES.put(2016, new FDBErrorInfo("FUTURE_NOT_ERROR", "Future not an error"));
        ERROR_CODES.put(2017, new FDBErrorInfo("USED_DURING_COMMIT", "Operation issued while a commit was outstanding"));
        ERROR_CODES.put(2018, new FDBErrorInfo("INVALID_MUTATION_TYPE", "Unrecognized atomic mutation type"));
        ERROR_CODES.put(2020, new FDBErrorInfo("TRANSACTION_INVALID_VERSION", "Transaction does not have a valid commit version"));
        ERROR_CODES.put(2021, new FDBErrorInfo("NO_COMMIT_VERSION", "Transaction is read-only and therefore does not have a commit version"));
        ERROR_CODES.put(2022, new FDBErrorInfo("ENVIRONMENT_VARIABLE_NETWORK_OPTION_FAILED", "Environment variable network option could not be set"));
        ERROR_CODES.put(2023, new FDBErrorInfo("TRANSACTION_READ_ONLY", "Attempted to commit a transaction specified as read-only"));
        ERROR_CODES.put(2024, new FDBErrorInfo("INVALID_CACHE_EVICTION_POLICY", "Invalid cache eviction policy, only random and lru are supported"));
        ERROR_CODES.put(2025, new FDBErrorInfo("NETWORK_CANNOT_BE_RESTARTED", "Network can only be started once"));
        ERROR_CODES.put(2026, new FDBErrorInfo("BLOCKED_FROM_NETWORK_THREAD", "Detected a deadlock in a callback called from the network thread"));
        ERROR_CODES.put(2100, new FDBErrorInfo("INCOMPATIBLE_PROTOCOL_VERSION", "Incompatible protocol version"));
        ERROR_CODES.put(2101, new FDBErrorInfo("TRANSACTION_TOO_LARGE", "Transaction exceeds byte limit"));
        ERROR_CODES.put(2102, new FDBErrorInfo("KEY_TOO_LARGE", "Key length exceeds limit"));
        ERROR_CODES.put(2103, new FDBErrorInfo("VALUE_TOO_LARGE", "Value length exceeds limit"));
        ERROR_CODES.put(2104, new FDBErrorInfo("CONNECTION_STRING_INVALID", "Connection string invalid"));
        ERROR_CODES.put(2105, new FDBErrorInfo("ADDRESS_IN_USE", "Local address in use"));
        ERROR_CODES.put(2106, new FDBErrorInfo("INVALID_LOCAL_ADDRESS", "Invalid local address"));
        ERROR_CODES.put(2107, new FDBErrorInfo("TLS_ERROR", "TLS error"));
        ERROR_CODES.put(2108, new FDBErrorInfo("UNSUPPORTED_OPERATION", "Operation is not supported"));
        ERROR_CODES.put(2109, new FDBErrorInfo("TOO_MANY_TAGS", "Too many tags set on transaction"));
        ERROR_CODES.put(2110, new FDBErrorInfo("TAG_TOO_LONG", "Tag set on transaction is too long"));
        ERROR_CODES.put(2111, new FDBErrorInfo("TOO_MANY_TAG_THROTTLES", "Too many tag throttles have been created"));
        ERROR_CODES.put(2112, new FDBErrorInfo("SPECIAL_KEYS_CROSS_MODULE_READ", "Special key space range read crosses modules"));
        ERROR_CODES.put(2113, new FDBErrorInfo("SPECIAL_KEYS_NO_MODULE_FOUND", "Special key space range read does not intersect a module"));
        ERROR_CODES.put(2114, new FDBErrorInfo("SPECIAL_KEYS_WRITE_DISABLED", "Special key space is not allowed to write by default"));
        ERROR_CODES.put(2115, new FDBErrorInfo("SPECIAL_KEYS_NO_WRITE_MODULE_FOUND", "Special key space key or keyrange does not intersect a module"));
        ERROR_CODES.put(2116, new FDBErrorInfo("SPECIAL_KEYS_CROSS_MODULE_WRITE", "Special key space clear crosses modules"));
        ERROR_CODES.put(2117, new FDBErrorInfo("SPECIAL_KEYS_API_FAILURE", "Api call through special keys failed"));
        ERROR_CODES.put(2130, new FDBErrorInfo("TENANT_NAME_REQUIRED", "Tenant name must be specified to access data in the cluster"));
        ERROR_CODES.put(2131, new FDBErrorInfo("TENANT_NOT_FOUND", "Tenant does not exist"));
        ERROR_CODES.put(2132, new FDBErrorInfo("TENANT_ALREADY_EXISTS", "A tenant with the given name already exists"));
        ERROR_CODES.put(2133, new FDBErrorInfo("TENANT_NOT_EMPTY", "Cannot delete a non-empty tenant"));
        ERROR_CODES.put(2134, new FDBErrorInfo("INVALID_TENANT_NAME", "Tenant name cannot begin with xff"));
        ERROR_CODES.put(2135, new FDBErrorInfo("TENANT_PREFIX_ALLOCATOR_CONFLICT", "The database already has keys stored at the prefix allocated for the tenant"));
        ERROR_CODES.put(2136, new FDBErrorInfo("TENANTS_DISABLED", "Tenants have been disabled in the cluster"));
        ERROR_CODES.put(2200, new FDBErrorInfo("API_VERSION_UNSET", "API version is not set"));
        ERROR_CODES.put(2201, new FDBErrorInfo("API_VERSION_ALREADY_SET", "API version may be set only once"));
        ERROR_CODES.put(2202, new FDBErrorInfo("API_VERSION_INVALID", "API version not valid"));
        ERROR_CODES.put(2203, new FDBErrorInfo("API_VERSION_NOT_SUPPORTED", "API version not supported"));
        ERROR_CODES.put(2210, new FDBErrorInfo("EXACT_MODE_WITHOUT_LIMITS", "EXACT streaming mode requires limits, but none were given"));
        ERROR_CODES.put(4000, new FDBErrorInfo("UNKNOWN_ERROR", "An unknown error occurred"));
        ERROR_CODES.put(4100, new FDBErrorInfo("INTERNAL_ERROR", "An internal error occurred"));
    }

    private FDBErrorRegistry() {
    }

    public static FDBErrorInfo lookup(int code) {
        return ERROR_CODES.get(code);
    }
}
