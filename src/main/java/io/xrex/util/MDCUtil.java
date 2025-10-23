package io.xrex.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.MDC;

public class MDCUtil {
    public static String getRequestId() {
        String requestId = MDC.get("request_id");
        if (requestId == null) {
            requestId = RandomStringUtils.randomAlphanumeric(8);
        }
        return requestId;
    }
}
