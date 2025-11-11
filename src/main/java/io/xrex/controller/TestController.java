package io.xrex.controller;

import io.grpc.stub.StreamObserver;
import io.xrex.grpc.*;
import io.xrex.service.grpc.TransferGrpcService;
import io.xrex.util.UUIDv7Generator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

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
            Integer toUid = ThreadLocalRandom.current().nextInt(19900, 19921);
            for (int j = 0; j < requestCount; j++) {
                Integer type;
                String amount;
                if (j % 3 == 0) {
                    type = 201101;
                    amount = "0.01";
                } else if (j % 3 == 1) {
                    type = 201102;
                    amount = "0.1";
                } else {
                    type = 201106;
                    amount = "100";
                }
                List<TransferRequest> requests = new ArrayList<>();
                TransferRequest request = TransferRequest.newBuilder()
                        .setFromUid(1).setFromType(type).setToUid(toUid).setToType(type)
                        .setAmount(amount).setScene("TRANSFER_COMMON").setMeta("0")
                        .setRefType("test").setRefId((i + j) % 2 == 0 ? 1 : 2).setOpUid(1).setOpIp("127.0.0.1")
                        .setRequestId(UUIDv7Generator.generate()).build();
                requests.add(request);
                TransferListRequest listRequest = TransferListRequest.newBuilder().addAllRequests(requests).build();
                try {
                    transferGrpcService.transfer(listRequest, responseObserver);
                } catch (Exception e) {
                    log.error("Test transfer failed", e);
                }
            }
            if (loop % 200 == 0) {
                Thread.sleep(10);
            }
        }
    }

    @PostMapping("/api/v2/test")
    public void test2() throws InterruptedException {
        StreamObserver<LedgerResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(LedgerResponse value) {
                StreamObserver<TransferResponse> observer = new StreamObserver<>() {
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
                TransferRequest request = TransferRequest.newBuilder()
                        .setFromUid(value.getChainupId()).setFromType(value.getType()).setToUid(1).setToType(value.getType())
                        .setAmount(value.getBalance()).setScene("TRANSFER_COMMON").setMeta("0")
                        .setRefType("test").setRefId(ThreadLocalRandom.current().nextInt(1, 3)).setOpUid(1).setOpIp("127.0.0.1")
                        .setRequestId(UUIDv7Generator.generate()).build();
                TransferListRequest listRequest = TransferListRequest.newBuilder().addAllRequests(List.of(request)).build();
                transferGrpcService.transfer(listRequest, observer);
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

        Integer uid = 19900;
        List<Integer> types = List.of(201101, 201102, 201106);
        while (uid < 20001) {
            for (Integer type : types) {
                LedgerRequest ledgerRequest = LedgerRequest.newBuilder().setChainupId(uid).setType(type).build();
                transferGrpcService.getLedgerFromMemory(ledgerRequest, responseObserver);
            }
            uid++;
        }
    }
}
