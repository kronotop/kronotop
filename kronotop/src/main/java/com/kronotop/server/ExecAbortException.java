/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.server;

import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;

/**
 * A custom exception class representing the case where a Redis transaction is aborted due to previous errors.
 * This exception extends the KronotopException class.
 */
public class ExecAbortException extends KronotopException {
    public ExecAbortException() {
        super(RESPError.EXECABORT, RESPError.EXECABORT_MESSAGE);
    }
}
