package io.xrex.controller;

import io.grpc.stub.StreamObserver;
import io.xrex.grpc.TransferListRequest;
import io.xrex.grpc.TransferRequest;
import io.xrex.grpc.TransferResponse;
import io.xrex.service.grpc.TransferGrpcService;
import io.xrex.util.UUIDv7Generator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
public class TestController {

    private final TransferGrpcService transferGrpcService;

    @PostMapping("/api/v1/test")
    public void test(@RequestParam(value = "request_count") int requestCount,
                     @RequestParam(value = "loop") int loop) throws InterruptedException {


        StreamObserver<TransferResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(TransferResponse value) {
                //log.info("Test transfer submitted to Raft: {}", value.getMessage());
            }

            @Override
            public void onError(Throwable t) {
                log.error("Test transfer error", t);
            }

            @Override
            public void onCompleted() {
                //log.info("Test transfer stream completed.");
            }
        };

        for (int i = 0; i < loop; i++) {
            long start = System.currentTimeMillis();
//            Integer toUid = ThreadLocalRandom.current().nextInt(19900, 20001);
            Integer toUid = 19914;
            for (int j = 0; j < requestCount; j++) {
                List<TransferRequest> requests = new ArrayList<>();
                TransferRequest request = TransferRequest.newBuilder()
                        .setFromUid(1).setFromType(201106).setToUid(toUid).setToType(201106)
                        .setAmount("100").setScene("TRANSFER_COMMON").setMeta("0")
                        .setRefType("test").setRefId(i % 2 == 0 ? 1 : 2).setOpUid(1).setOpIp("127.0.0.1")
                        .setRequestId(UUIDv7Generator.generate()).build();
                requests.add(request);
                TransferListRequest listRequest = TransferListRequest.newBuilder().addAllRequests(requests).build();
                try {
                    transferGrpcService.transfer(listRequest, responseObserver);
                } catch (Exception e) {
                    log.error("Test transfer failed", e);
                }
            }
            log.info("Finished submitting {} requests, loop={}, time={}ms", requestCount, loop, System.currentTimeMillis() - start);
            if (loop % 200 == 0) {
                Thread.sleep(10);
            }
        }
    }
}
