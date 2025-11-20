package io.xrex.service;

import io.grpc.stub.StreamObserver;
import io.xrex.dto.PairConfigDto;
import io.xrex.enums.*;
import io.xrex.grpc.TransferListRequest;
import io.xrex.grpc.TransferRequest;
import io.xrex.grpc.TransferResponse;
import io.xrex.persistence.entity.ConfigAccountTypeEntity;
import io.xrex.persistence.entity.ExOrderEntity;
import io.xrex.persistence.repository.ConfigAccountTypeRepository;
import io.xrex.persistence.repository.ExOrderDao;
import io.xrex.service.grpc.TransferGrpcService;
import io.xrex.util.UUIDv7Generator;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

import static io.xrex.util.XrexConstant.SYSTEM_CHAINUP_ID;

@Service
@RequiredArgsConstructor
public class RemainReturnTransferService {

    private final TransferGrpcService transferGrpcService;
    private final ExOrderDao exOrderDao;
    private final ConfigAccountTypeRepository configAccountTypeRepository;

    @Async
    @Transactional
    public void handleRemainMoney(ExOrderEntity exOrder, PairConfigDto pairConfig, StreamObserver<TransferResponse> responseObserver) {
        if (exOrder == null || OrderLeverType.MARKET_MAKING_ORDER == exOrder.getOrderType()) {
            return;
        }

        if (exOrder.isMarginOrder()) {
            if (exOrder.isFilled(pairConfig)) {
                exOrderDao.updateStatus(exOrder.getId(), OrderStatus.FILLED, pairConfig.getOrderTable());
            }
            return;
        }

        if (!exOrder.isFilled(pairConfig)) {
            return;
        }

        BigDecimal remainAmount = exOrder.getRemainAmount();
        if (remainAmount.compareTo(BigDecimal.ZERO) > 0) {
            Pair<Integer, Integer> remainAccountTypes = getRemainAccountTypes(exOrder, pairConfig);
            TransferRequest.Builder trans = TransferRequest.newBuilder()
                    .setFromUid(exOrder.getUserId()).setFromType(remainAccountTypes.getLeft())
                    .setToUid(exOrder.getUserId()).setToType(remainAccountTypes.getRight())
                    .setAmount(remainAmount.toPlainString())
                    .setRefType(pairConfig.getOrderTable())
                    .setRefId(exOrder.getId())
                    .setMeta(getRemainMeta(exOrder))
                    .setScene(TransactionScene.CANCEL_TRADE.value)
                    .setOpUid(SYSTEM_CHAINUP_ID)
                    .setOpIp(StringUtils.EMPTY);
            if (exOrder.isRelatedSubAccount()) {
                String subAccountType = OrderSide.BUY == exOrder.getSide() ? exOrder.getQuoteSubaccountType() : exOrder.getBaseSubaccountType();
                trans.setFromSubType(subAccountType);
                trans.setToSubType(subAccountType);
            }
            TransferListRequest remainReturnTransferListRequest = TransferListRequest.newBuilder().addAllRequests(List.of(trans.build())).setRequestId(UUIDv7Generator.generate()).build();
            transferGrpcService.transfer(remainReturnTransferListRequest, responseObserver);
        }
    }

    private String getRemainMeta(ExOrderEntity exOrder) {
        String meta;
        if (exOrder.getSide() == OrderSide.BUY) {
            if (exOrder.isMarketOrder()) {
                meta = "order.unlock.returnMarketOrderQuoteAmount";
            } else {
                meta = "order.unlock.remainQuoteAmount";
            }
        } else {
            if (exOrder.isMarketOrder()) {
                meta = "order.unlock.returnMarketOrderBaseAmount";
            } else {
                meta = "order.unlock.remainBaseAmount";
            }
        }
        return meta;
    }

    private Pair<Integer, Integer> getRemainAccountTypes(ExOrderEntity exOrder, PairConfigDto pairConfig) {
        Integer fromType;
        Integer toType;
        if (OrderSide.BUY == exOrder.getSide()) {
            if (exOrder.isGridOrder() || exOrder.isGridMarginOrder()) {
                // Grid or Grid Margin
                fromType = exOrder.getQuoteAccountType();
                AssetType_A_BC assetType = exOrder.isGridOrder() ? AssetType_A_BC.U_GRID_NORMAL : AssetType_A_BC.U_GRID_MARGIN_NORMAL;

                String quoteCoin = "'" + pairConfig.getQuote().toUpperCase() + "'";
                ConfigAccountTypeEntity configAccountType = configAccountTypeRepository.findByAssetAAndAssetBcAndCoinSymbol(assetType.account_A,
                        assetType.account_BC, quoteCoin);
                toType = configAccountType.getAssetType();
            } else if (exOrder.isMarginOrder()) {
                fromType = exOrder.getQuoteAccountType();
                toType = pairConfig.getQuoteAccountNormal();
            } else if (exOrder.isConvertOrder() && exOrder.getQuoteAccountType() != null) {
                fromType = exOrder.getQuoteAccountType();
                toType = pairConfig.getQuoteAccountNormal();
            } else {
                fromType = pairConfig.getQuoteAccountLock();
                toType = pairConfig.getQuoteAccountNormal();
            }
        } else {
            if (exOrder.isGridOrder() || exOrder.isGridMarginOrder()) {
                // Grid
                fromType = exOrder.getBaseAccountType();
                AssetType_A_BC assetType = exOrder.isGridOrder() ? AssetType_A_BC.U_GRID_NORMAL : AssetType_A_BC.U_GRID_MARGIN_NORMAL;

                String baseCoin = "'" + pairConfig.getBase().toUpperCase() + "'";
                ConfigAccountTypeEntity configAccountType = configAccountTypeRepository.findByAssetAAndAssetBcAndCoinSymbol(assetType.account_A,
                        assetType.account_BC, baseCoin);
                toType = configAccountType.getAssetType();
            } else if (exOrder.isMarginOrder()) {
                fromType = exOrder.getBaseAccountType();
                toType = pairConfig.getBaseAccountNormal();
            } else if (exOrder.isConvertOrder() && exOrder.getBaseAccountType() != null) {
                fromType = exOrder.getBaseAccountType();
                toType = pairConfig.getBaseAccountNormal();
            } else {
                fromType = pairConfig.getBaseAccountLock();
                toType = pairConfig.getBaseAccountNormal();
            }
        }
        return Pair.of(fromType, toType);
    }
}
