package io.xrex.enums;

import lombok.Getter;

/**
 * 91000 ~ 91999
 */
public enum ErrorCodes {

    // common (0, 10001 ~ 19999, 100001 ~ 199999)
    SUCCESS("0", "suc"),
    SYSTEM_ERROR("10001", "System error"),
    NOT_LOGIN("10002", "User not login"),
    PARAMETER_ERROR("10020", "Parameter error"),
    BALANCE_NOT_ENOUGH("10034", "Balance not enough"),

    // Account Service error code
    BOOKKEEPING_FAILED("203001", "bookkeeping failed"),
    ;


    @Getter
    private final String code;
    @Getter
    private final String description;

    ErrorCodes(String code, String description) {
        this.code = code;
        this.description = description;
    }
}