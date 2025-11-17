package io.xrex.controller;


import io.xrex.enums.ErrorCodes;
import io.xrex.controller.exception.RestApiException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@ControllerAdvice
public class GlobalExceptionHandler {

    /**
     * Handle app exception and children
     */
    @ExceptionHandler({RestApiException.class})
    public ResponseEntity<RestApiResponse<?>> handleRestApiException(RestApiException exception) {
        log.info("[handleRestApiException] error: {}", exception.toString(), exception);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new ResponseEntity<>(new RestApiResponse<>(exception.getApiRes().getCode(), exception.getApiRes().getDesc()), headers, HttpStatus.OK);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<RestApiResponse<?>> handleException(MethodArgumentNotValidException exception) {
        log.error("RestController MethodArgumentNotValidException happens, errorMsg={}", exception.getMessage(), exception);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        // format the error field
        Map<String, String> errorFieldMap = exception.getBindingResult().getFieldErrors().stream()
                .collect(Collectors.toMap(fieldError -> camelToSnakeCase(fieldError.getField()), error -> Optional.ofNullable(error.getDefaultMessage()).orElse("field is invalid")));

        return new ResponseEntity<>(RestApiResponse.create(ErrorCodes.PARAMETER_ERROR, errorFieldMap), headers, HttpStatus.OK);
    }

    @ExceptionHandler({Exception.class})
    public ResponseEntity<RestApiResponse<?>> handleException(Exception exception) {
        log.warn("[handleException] error: ", exception);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new ResponseEntity<>(new RestApiResponse<>(ErrorCodes.SYSTEM_ERROR.getCode(), exception.getMessage()), headers, HttpStatus.OK);
    }

    @ExceptionHandler({MissingServletRequestParameterException.class})
    public ResponseEntity<RestApiResponse<?>> handleMissingServletRequestParameterException(MissingServletRequestParameterException exception) {
        log.warn("[handleMissingServletRequestParameterException] error: {}", exception.getMessage());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new ResponseEntity<>(new RestApiResponse<>(ErrorCodes.PARAMETER_ERROR.getCode(), exception.getMessage()), headers, HttpStatus.OK);
    }

    @ExceptionHandler({HttpMessageNotReadableException.class})
    public ResponseEntity<RestApiResponse<?>> handleHttpMessageNotReadableException(HttpMessageNotReadableException exception) {
        log.warn("[handleHttpMessageNotReadableException] error: {}", exception.getMessage());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new ResponseEntity<>(new RestApiResponse<>(ErrorCodes.PARAMETER_ERROR), headers, HttpStatus.OK);
    }

    private String camelToSnakeCase(final String str) {
        if (StringUtils.isBlank(str)) {
            return StringUtils.EMPTY;
        }
        return str.replaceAll("\\B([A-Z])", "_$1").toLowerCase();
    }
}
