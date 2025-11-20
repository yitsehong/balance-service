package io.xrex.controller;

import io.grpc.stub.StreamObserver;
import io.xrex.dto.event.ExTradeDto;
import io.xrex.dto.event.TradeEventDto;
import io.xrex.enums.*;
import io.xrex.grpc.*;
import io.xrex.persistence.entity.ExOrderEntity;
import io.xrex.persistence.entity.ExTradeEntity;
import io.xrex.persistence.repository.ExOrderDao;
import io.xrex.persistence.repository.ExTradeDao;
import io.xrex.service.grpc.TransferGrpcService;
import io.xrex.util.UUIDv7Generator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@RestController
@RequiredArgsConstructor
public class TestController {

    private final TransferGrpcService transferGrpcService;
    private final KafkaTemplate<String, TradeEventDto> testKafkaTemplate;
    private final ExTradeDao exTradeDao;
    private final ExOrderDao exOrderDao;

    @Value("${app.kafka.trade-event.topic}")
    private String tradeEventTopic;
    @Value("${balance-service.mm-chainup-id}")
    private Integer mmChainupId;

    @PostMapping("/api/v1/test")
    public void test(@RequestParam(value = "request_count") int requestCount,
                     @RequestParam(value = "loop") int loop) throws InterruptedException {

        StreamObserver<TransferResponse> responseObserver = build();
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
                        .build();
                requests.add(request);
                TransferListRequest listRequest = TransferListRequest.newBuilder().addAllRequests(requests).setRequestId(UUIDv7Generator.generate()).build();
                try {
                    transferGrpcService.transfer(listRequest, responseObserver);
                } catch (Exception e) {
                    log.error("Test transfer failed", e);
                }
            }
            Thread.sleep(500);
        }
    }

    @PostMapping("/api/v1/test_trade")
    public void testTrade(@RequestParam(value = "loop") int loop) {
        String base = "btc";
        String quote = "twd";
        String pair = base + quote;
        Integer fromType = 2011013;
        Integer toType = 2021013;
        BigDecimal feeRate = new BigDecimal("0.001");

        String orderTable = "ex_order_" + pair;
        String tradeTable = "ex_trade_" + pair;
        Integer chainupId = 19914;
        ExTradeEntity latestTrade = exTradeDao.findLatestTrade(tradeTable);
        BigDecimal tradePrice = latestTrade.getPrice();
        StreamObserver<TransferResponse> responseObserver = build();
        for (int i = 0; i < loop; i++) {
            BigDecimal spendMoney = new BigDecimal("1000");
            LocalDateTime now = LocalDateTime.now();
            BigDecimal volume = spendMoney.divide(latestTrade.getPrice(), 10, RoundingMode.DOWN);
            BigDecimal dealMoney = tradePrice.multiply(volume);
            ExOrderEntity marketOrder = ExOrderEntity.builder()
                    .userId(chainupId).side(OrderSide.BUY)
                    .price(BigDecimal.ZERO).volume(spendMoney)
                    .feeDeductType(FeeDeductType.INNER).feeRateTaker(feeRate.doubleValue()).feeRateMaker(feeRate.doubleValue())
                    .fee(BigDecimal.ZERO).feeCoinRate(BigDecimal.ZERO.doubleValue()).dealVolume(BigDecimal.ZERO).dealMoney(BigDecimal.ZERO).avgPrice(BigDecimal.ZERO).lockedAmount(spendMoney)
                    .status(OrderStatus.INIT).type(OrderType.MARKET).ctime(now).mtime(now).source(OrderSourceType.WEB).orderType(OrderLeverType.NORMAL_ORDER)
                    .build();
            ExOrderEntity mmOrder = ExOrderEntity.builder()
                    .userId(mmChainupId).side(OrderSide.SELL)
                    .price(tradePrice).volume(volume)
                    .feeDeductType(FeeDeductType.INNER).feeRateTaker(BigDecimal.ZERO.doubleValue()).feeRateMaker(BigDecimal.ZERO.doubleValue())
                    .fee(BigDecimal.ZERO).feeCoinRate(BigDecimal.ZERO.doubleValue()).dealVolume(volume).dealMoney(dealMoney).avgPrice(tradePrice).lockedAmount(volume)
                    .status(OrderStatus.FILLED).type(OrderType.LIMIT).ctime(now).mtime(now).source(OrderSourceType.WEB).orderType(OrderLeverType.MARKET_MAKING_ORDER)
                    .build();
            List<Long> orderIds = exOrderDao.batchInsert(List.of(marketOrder, mmOrder), orderTable);
            Long orderId = orderIds.getFirst();

            List<TransferRequest> requests = new ArrayList<>();
            TransferRequest request = TransferRequest.newBuilder()
                    .setFromUid(chainupId).setFromType(fromType).setToUid(chainupId).setToType(toType)
                    .setAmount(spendMoney.toPlainString()).setScene(TransactionScene.CREATE_ORDER.value).setMeta("0")
                    .setRefType(orderTable).setRefId(orderId).setOpUid(chainupId).setOpIp(StringUtils.EMPTY)
                    .build();
            requests.add(request);
            TransferListRequest listRequest = TransferListRequest.newBuilder().addAllRequests(requests).setRequestId(UUIDv7Generator.generate()).build();
            transferGrpcService.transfer(listRequest, responseObserver);

            TradeEventDto tradeEventDto = TradeEventDto.builder()
                    .pair(pair)
                    .trade(ExTradeDto.builder()
                            .price(latestTrade.getPrice())
                            .volume(volume)
                            .bidId(orderId)
                            .askId(orderIds.getLast())
                            .trendSide(marketOrder.getSide().value)
                            .bidUserId(chainupId)
                            .askUserId(mmChainupId)
                            .buyFee(BigDecimal.ZERO)
                            .sellFee(BigDecimal.ZERO)
                            .buyFeeCoin(null)
                            .sellFeeCoin(null)
                            .ctime(now).mtime(now)
                            .buyType(OrderLeverType.NORMAL_ORDER.value)
                            .sellType(OrderLeverType.MARKET_MAKING_ORDER.value)
                            .tradeNonce(UUIDv7Generator.generate())
                            .build())
                    .eventTime(LocalDateTime.now()).build();
            testKafkaTemplate.send(tradeEventTopic, tradeTable, tradeEventDto);
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
                        .setFromUid(value.getData().getChainupId()).setFromType(value.getData().getType()).setToUid(1).setToType(value.getData().getType())
                        .setAmount(value.getData().getBalance()).setScene("TRANSFER_COMMON").setMeta("0")
                        .setRefType("test").setRefId(ThreadLocalRandom.current().nextInt(1, 3)).setOpUid(1).setOpIp("127.0.0.1")
                        .build();
                TransferListRequest listRequest = TransferListRequest.newBuilder().addAllRequests(List.of(request)).setRequestId(UUIDv7Generator.generate()).build();
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

    private StreamObserver<TransferResponse> build() {
        return new StreamObserver<>() {
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

    }
}
