package io.xrex.controller;

import io.grpc.stub.StreamObserver;
import io.xrex.grpc.TransferListRequest;
import io.xrex.grpc.TransferRequest;
import io.xrex.grpc.TransferResponse;
import io.xrex.service.TransferInMemoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
public class TestController {

    private final TransferInMemoryService transferInMemoryService;

    @PostMapping("/api/v1/test/{totalRequests}")
    public void test(@PathVariable int totalRequests) throws InterruptedException {
        long start = System.currentTimeMillis();

        StreamObserver<TransferResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(TransferResponse value) {
                log.info("Test transfer accepted: {}", value.getEventKeyList());
            }

            @Override
            public void onError(Throwable t) {
                log.error("Test transfer error", t);
            }

            @Override
            public void onCompleted() {
                log.info("Test transfer stream completed.");
            }
        };

        List<TransferRequest> requests = new ArrayList<>();
        for (int i = 0; i < totalRequests; i++) {
            TransferRequest request = TransferRequest.newBuilder()
                    .setFromUid(1).setFromType(201106).setToUid(19914).setToType(201106)
                    .setAmount("100").setScene("TRANSFER_COMMON").setMeta("0")
                    .setRefType("test").setRefId(i).setOpUid(1).setOpIp("127.0.0.1").build();
            requests.add(request);
        }

        TransferListRequest listRequest = TransferListRequest.newBuilder().addAllRequests(requests).build();

        try {
            transferInMemoryService.transfer(listRequest, responseObserver);
        } catch (Exception e) {
            log.error("Test transfer failed", e);
        }

        log.info("Finished sending {} requests, time={}ms", 2, System.currentTimeMillis() - start);
    }
}
