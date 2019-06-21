package com.mtl.locks;

/**
 * 说明:
 *
 * @作者 莫天龙
 * @时间 2019/06/21 15:01
 */
public class IllegalLockOwerException extends RuntimeException {
    public IllegalLockOwerException() {
    }

    public IllegalLockOwerException(String message) {
        super(message);
    }

    public IllegalLockOwerException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalLockOwerException(Throwable cause) {
        super(cause);
    }

    public IllegalLockOwerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
