package io.xrex.service;

import io.xrex.dto.CancelOrderIdDto;
import io.xrex.dto.PairConfigDto;
import io.xrex.enums.*;
import io.xrex.grpc.TransferListRequest;
import io.xrex.grpc.TransferRequest;
import io.xrex.persistence.entity.ConfigAccountTypeEntity;
import io.xrex.persistence.entity.ExOrderEntity;
import io.xrex.persistence.repository.ConfigAccountTypeRepository;
import io.xrex.persistence.repository.ExOrderDao;
import io.xrex.util.UUIDv7Generator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.xrex.util.XrexConstant.SYSTEM_CHAINUP_ID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderTransferService {

    private final ConfigService configService;

    private final ExOrderDao exOrderDao;
    private final ConfigAccountTypeRepository configAccountTypeRepository;

    @Transactional
    public TransferListRequest handleCancelOrderTransfer(CancelOrderIdDto cancelOrderId, List<Long> cancelOrderIds) {
        PairConfigDto pairConfig = configService.findPairConfigByPair(cancelOrderId.pair());
        List<ExOrderEntity> exOrders = exOrderDao.findByIdIn(cancelOrderIds, pairConfig.getOrderTable());
        if (exOrders.isEmpty()) {
            return TransferListRequest.newBuilder().build();
        }

        Map<OrderSide, List<ExOrderEntity>> cancelOrders = new HashMap<>();
        for (ExOrderEntity exOrder : exOrders) {
            List<ExOrderEntity> sideCancelOrders = cancelOrders.getOrDefault(exOrder.getSide(), new ArrayList<>());
            sideCancelOrders.add(exOrder);
            cancelOrders.put(exOrder.getSide(), sideCancelOrders);
        }

        Map<String, Pair<Integer, Integer>> cancelOrderAccountTypes = getCancelOrderAccountTypes(cancelOrderId.orderType(), pairConfig,
                exOrders.getFirst().getQuoteAccountType(), exOrders.getFirst().getBaseAccountType());
        List<TransferRequest> requests = new ArrayList<>();
        for (Map.Entry<OrderSide, List<ExOrderEntity>> entry : cancelOrders.entrySet()) {
            String coinSymbol = OrderSide.BUY == entry.getKey() ? pairConfig.getQuote() : pairConfig.getBase();
            Pair<Integer, Integer> cancelAccountTypes = cancelOrderAccountTypes.get(coinSymbol);
            for (ExOrderEntity cancelOrder : entry.getValue()) {
                TransferRequest.Builder transD = TransferRequest.newBuilder()
                        .setFromUid(cancelOrder.getUserId()).setFromType(cancelAccountTypes.getLeft())
                        .setToUid(cancelOrder.getUserId()).setToType(cancelAccountTypes.getRight())
                        .setAmount(cancelOrder.getRemainAmount().toPlainString())
                        .setRefType(pairConfig.getOrderTable())
                        .setRefId(cancelOrder.getId())
                        .setMeta("fund.transaction.scene.cancel.order")
                        .setScene(TransactionScene.CANCEL_ORDER.value)
                        .setOpUid(SYSTEM_CHAINUP_ID)
                        .setOpIp(StringUtils.EMPTY);

                if (cancelOrder.isRelatedSubAccount()) {
                    if (OrderSide.BUY == cancelOrder.getSide()) {
                        transD.setFromSubType(cancelOrder.getQuoteSubaccountType());
                        transD.setToSubType(cancelOrder.getQuoteSubaccountType());
                    } else {
                        transD.setFromSubType(cancelOrder.getBaseSubaccountType());
                        transD.setToSubType(cancelOrder.getBaseSubaccountType());
                    }
                }
                requests.add(transD.build());
            }
        }
        exOrderDao.batchUpdateCancelStatus(cancelOrderIds, OrderStatus.CANCELED, pairConfig.getOrderTable());
        return TransferListRequest.newBuilder().addAllRequests(requests).setRequestId(UUIDv7Generator.generate()).build();
    }

    private Map<String, Pair<Integer, Integer>> getCancelOrderAccountTypes(OrderLeverType orderType, PairConfigDto pairConfig,
                                                                           Integer orderQuoteAccountType, Integer orderBaseAccountType) {
        List<String> coinSymbols = List.of(pairConfig.getBase(), pairConfig.getQuote());
        Map<String, Pair<Integer, Integer>> result = new HashMap<>();
        for (String coinSymbol : coinSymbols) {
            Integer fromType;
            Integer toType;
            if (OrderLeverType.GRID_ORDER == orderType || OrderLeverType.GRID_MARGIN_ORDER == orderType) {
                // Grid or Grid Margin
                fromType = pairConfig.getQuote().equalsIgnoreCase(coinSymbol) ? orderQuoteAccountType : orderBaseAccountType;
                AssetType_A_BC assetType = OrderLeverType.GRID_ORDER == orderType ? AssetType_A_BC.U_GRID_NORMAL : AssetType_A_BC.U_GRID_MARGIN_NORMAL;
                ConfigAccountTypeEntity configAccountType = configAccountTypeRepository.findByAssetAAndAssetBcAndCoinSymbol(assetType.account_A,
                        assetType.account_BC, coinSymbol.toUpperCase());
                toType = configAccountType.getAssetType();
            } else if (OrderLeverType.MARGIN_ORDER == orderType) {
                fromType = pairConfig.getQuote().equalsIgnoreCase(coinSymbol) ? orderQuoteAccountType : orderBaseAccountType;
                toType = pairConfig.getQuote().equalsIgnoreCase(coinSymbol) ? pairConfig.getQuoteAccountNormal() : pairConfig.getBaseAccountNormal();
            } else {
                fromType = pairConfig.getQuote().equalsIgnoreCase(coinSymbol) ? pairConfig.getQuoteAccountLock() : pairConfig.getBaseAccountLock();
                toType = pairConfig.getQuote().equalsIgnoreCase(coinSymbol) ? pairConfig.getQuoteAccountNormal() : pairConfig.getBaseAccountNormal();
            }
            result.put(coinSymbol, Pair.of(fromType, toType));
        }
        return result;
    }
}
