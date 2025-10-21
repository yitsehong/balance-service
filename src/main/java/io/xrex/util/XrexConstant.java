package io.xrex.util;

public final class XrexConstant {

    public static final Integer SYSTEM_CHAINUP_ID = 1;

    public static final String ISO_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    /**
     * HTTP POST请求
     */
    public static final String POST = "POST";
    /**
     * UTF-8编码
     */
    public static final String UTF8 = "UTF-8";
    /**
     * The same as database column settings
     */
    public static final int DEFAULT_DECIMAL_PLACES = 16;

    public static final String TRANSFER_EVENT_TOPIC = "t.account-service.account-transfer-events";


    public static final String MM_CHAINUP_ID_KEY = "account_service:transfer:mm_chainup_id";
    public static final String MM_POOL_CHAINUP_ID_KEY = "account_service:transfer:mm_pool_chainup_id";
    public static final String IGNORE_CHECK_BALANCE_CHAINUP_ID_KEY = "account_service:transfer:ignore_check_balance_chainup_id";

}
