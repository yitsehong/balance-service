package io.xrex.controller;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.xrex.enums.ErrorCodes;
import io.xrex.util.MDCUtil;
import io.xrex.util.XrexConstant;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.domain.Page;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class RestApiResponse<T> implements Serializable {

    private String code;
    private String desc;
    private T data;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = XrexConstant.ISO_DATE_FORMAT)
    private Date timestamp;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Object extra;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer page;  //Current page start from 1
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer size;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer totalPages;  // Total pages start from 1
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long totalCount;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String requestId;

    public RestApiResponse() {
    }

    @Builder
    public RestApiResponse(String code, String description, T data, Object extra) {
        this.timestamp = new Date();
        this.code = code;
        this.desc = description;
        this.data = data;
        this.extra = extra;
        this.requestId = MDCUtil.getRequestId();
    }

    @Builder
    public RestApiResponse(ErrorCodes errCode, T data) {
        new RestApiResponse<>(errCode, data, null);
    }

    @Builder
    public RestApiResponse(ErrorCodes errCode) {
        this(errCode, null, null);
    }

    @Builder
    private RestApiResponse(ErrorCodes errCode, T data, Object extra) {
        this.timestamp = new Date();
        this.code = errCode.getCode();
        this.desc = errCode.getDescription();
        this.data = data;
        this.extra = extra;
        this.requestId = MDCUtil.getRequestId();
    }

    @Builder
    public RestApiResponse(String code, String description) {
        this.timestamp = new Date();
        this.code = code;
        this.desc = description;
        this.requestId = MDCUtil.getRequestId();
    }

    @Builder
    public RestApiResponse(String code, String description, Object extra) {
        this.timestamp = new Date();
        this.code = code;
        this.desc = description;
        this.extra = extra;
        this.requestId = MDCUtil.getRequestId();
    }

    public static <T> RestApiResponse<T> ok() {
        return new RestApiResponse<>(ErrorCodes.SUCCESS, null, null);
    }

    public static <T> RestApiResponse<T> ok(T data) {
        return new RestApiResponse<>(ErrorCodes.SUCCESS, data, null);
    }

    public static <T> RestApiResponse<T> create(ErrorCodes errCode) {
        return new RestApiResponse<>(errCode.getCode(), errCode.getDescription(), null, null);
    }

    public static <T> RestApiResponse<T> create(String errCode, String errMsg) {
        return new RestApiResponse<>(errCode, errMsg, null, null);
    }

    public static <T> RestApiResponse<T> create(ErrorCodes errCode, T data) {
        return new RestApiResponse<>(errCode.getCode(), errCode.getDescription(), data, null);
    }

    public static <T> RestApiResponse<T> create(String code, String description, T data, Object extra) {
        return new RestApiResponse<>(code, description, data, extra);
    }

    public static <T> RestApiResponse<List<T>> createFromPage(Page<T> data) {
        return RestApiResponse.build(data);
    }

    public static <T, R> RestApiResponse<List<R>> createFromPage(Page<T> data, Function<T, R> convertor) {
        return RestApiResponse.build(data, convertor);
    }

    public static <T> RestApiResponse<List<T>> build(Page<T> page) {
        RestApiResponse<List<T>> resp = RestApiResponse.create(ErrorCodes.SUCCESS, page.getContent());
        resp.setPage(page.getNumber() + 1);
        resp.setSize(page.getSize());
        resp.setTotalPages(page.getTotalPages() == 0 ? 1 : page.getTotalPages());
        resp.setTotalCount(page.getTotalElements());
        resp.setRequestId(MDCUtil.getRequestId());
        return resp;
    }

    public static <T, R> RestApiResponse<List<R>> build(Page<T> page, Function<T, R> convertor) {
        List<R> convertToNewContent = page.getContent().stream().map(convertor).collect(Collectors.toList());
        RestApiResponse<List<R>> resp = RestApiResponse.create(ErrorCodes.SUCCESS, convertToNewContent);
        resp.setPage(page.getNumber() + 1);
        resp.setSize(page.getSize());
        resp.setTotalPages(page.getTotalPages() == 0 ? 1 : page.getTotalPages());
        resp.setTotalCount(page.getTotalElements());
        resp.setRequestId(MDCUtil.getRequestId());
        return resp;
    }

    @JsonIgnore
    public boolean isSuccess() {
        return "0".equals(code);
    }

}
