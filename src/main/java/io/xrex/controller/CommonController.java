package io.xrex.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/common", produces = "application/json;charset=utf-8")
public class CommonController {

    @GetMapping(value = "/health")
    public RestApiResponse<Void> getHealth() {
        return RestApiResponse.ok();
    }
}
