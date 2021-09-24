/*
 *    Copyright 2021 Two Sigma Open Source, LLC
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.iceberg.jdbc.v2;

/** For unknown error, call the custom constructor with essMsg and throwable */
public class CatalogDbException extends RuntimeException {
    public enum ErrorCode {
        NAMESPACE_NOT_FOUND {
            @Override
            public String getErrorMsg(String namespaceName, String tableName, String errorMsg) {
                return String.format("Namespace %s not found", namespaceName);
            }
        },
        NAMESPACE_EXISTS {
            @Override
            public String getErrorMsg(String namespaceName, String tableName, String errorMsg) {
                return String.format("Namespace %s already exists", namespaceName);
            }
        },
        NAMESPACE_NOT_FOUND_OR_STALE {
            @Override
            public String getErrorMsg(String namespaceName, String tableName, String errorMsg) {
                return String.format("Namespace %s not found or it is stale", namespaceName);
            }
        },
        TABLE_NOT_FOUND {
            @Override
            public String getErrorMsg(String namespaceName, String tableName, String errorMsg) {
                return String.format(
                        "Table %s not found in namespace %s", tableName, namespaceName);
            }
        },
        TABLE_EXISTS {
            @Override
            public String getErrorMsg(String namespaceName, String tableName, String errorMsg) {
                return String.format(
                        "Table %s already exists in namespace %s", tableName, namespaceName);
            }
        },
        TABLE_NOT_FOUND_OR_STALE {
            @Override
            public String getErrorMsg(String namespaceName, String tableName, String errorMsg) {
                return String.format(
                        "Table %s not found in namespace %s or the table is stale",
                        tableName, namespaceName);
            }
        },
        UNKNOWN_ERR {
            @Override
            public String getErrorMsg(String namespaceName, String tableName, String errorMsg) {
                return errorMsg;
            }
        };

        public abstract String getErrorMsg(String namespaceName, String tableName, String errorMsg);
    }

    private final ErrorCode errorCode;
    private final String localErrorMessage;

    public CatalogDbException(Throwable e) {
        this(ErrorCode.UNKNOWN_ERR, null, null, e, null);
    }

    /** Constructor with custom exception message */
    public CatalogDbException(String errMsg, Throwable e) {
        this(ErrorCode.UNKNOWN_ERR, null, null, e, errMsg);
    }

    /** Use for creating exception only related to namespaces operations */
    public CatalogDbException(ErrorCode errorCode, String namespaceName, Throwable e) {
        this(errorCode, namespaceName, null, e);
    }

    public CatalogDbException(
            ErrorCode errorCode, String namespaceName, String tableName, Throwable e) {
        this(errorCode, namespaceName, tableName, e, null);
    }

    public CatalogDbException(
            ErrorCode errorCode,
            String namespaceName,
            String tableName,
            Throwable e,
            String errMsg) {
        super(e);
        this.errorCode = errorCode;
        this.localErrorMessage = errorCode.getErrorMsg(namespaceName, tableName, errMsg);
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        if (localErrorMessage != null) {
            return localErrorMessage;
        } else {
            return super.getMessage();
        }
    }
}
