package io.xrex.controller.exception;


import io.xrex.controller.RestApiResponse;
import io.xrex.enums.ErrorCodes;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class RestApiException extends RuntimeException {

    RestApiResponse<?> apiRes;

    public RestApiException(RestApiResponse<?> apiRes) {
        super(apiRes.toString());
        this.apiRes = apiRes;
    }

    public RestApiException(String code, String description) {
        this(RestApiResponse.builder().code(code).description(description).build());
    }

    public RestApiException(String code) {
        this(RestApiResponse.builder().code(code).build());
    }

    public RestApiException(ErrorCodes code, String description) {
        this(RestApiResponse.builder().code(code.getCode()).description(description).build());
    }

    public RestApiException(ErrorCodes code) {
        this(RestApiResponse.builder().code(code.getCode()).description(code.getDescription()).build());
    }

}
