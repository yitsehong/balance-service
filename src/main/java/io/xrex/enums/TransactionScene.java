package io.xrex.enums;

public enum TransactionScene {
    TRADE("trade", "fund.transaction.scene.trade", "交易,包括买入和卖出"),
    BUY("buy", "trade.buy", "买入,前台展示时使用"),
    SELL("sell", "trade.sell", "卖出,前台展示时使用"),
    DEPOSIT("deposit", "fund.transaction.scene.deposit", "充值"),
    WITHDRAW_SUCCESS("withdraw_success", "fund.transaction.scene.withdraw.success", "提现成功"),
    WITHDRAW("withdraw", "fund.transaction.scene.withdraw", "提现中"),
    WITHDRAW_CNACEL("withdraw_cnacel", "fund.transaction.scene.withdraw.cnacel", "取消提现"),
    CREATE_ORDER("create_order", "fund.transaction.scene.create.order", "下单"),
    CANCEL_TRADE("cancel_trade", "fund.transaction.scene.cancel.trade", "撤销订单"),
    CANCEL_ORDER("cancel_order", "fund.transaction.scene.cancel.order", "撤销订单"),
    PRESENT_COIN("present_coin", "fund.transaction.scene.present", "赠币"),
    CANCEL_EXPIRED_ORDER("cancel_expired_order", "", "撤销异常订单"),
    OPEN_API_CHARGE("open_api_charge", "", "Open API 充值"),
    OPEN_API_TRANSFER("open_api_tranfer", "", "Open API 转账"),
    OPEN_API_WITHDRAW("open_api_withdraw", "", "Open API 提现"),

    CREATE_SELL_AD("create_sell_ad", "release.sales.advertisement", "场外发布售卖广告"),
    OTC_TRANSFER_SELLAD("otc_transfer_sellad", "transfer.coin.sellad", "场外释放货币转账,出售广告"),
    OTC_ADVERT_CLOSE("otc_advert_close", "transfer.advert.close", "关闭广告,冻结账户余额解冻"),
    OTC_SELL_ORDER("otc_sell_order", "otc.sell.order", "生成出售订单,冻结出售方的账户余额"),
    OTC_TRANSFER_BUYAD("otc_transfer_buyad", "transfer.coin.buyad", "场外释放货币转账,求购广告"),
    OTC_INTERNAL_TRANSFER("otc_internal_transfer", "otc.internal.transfer", "场外资金划转到现货账户"),
    OTC_TRANSFER("otc_transfer", "otc.transfer", "场外资金划转"),
    OTC_COMPANY_MARGIN_RELEASE("otc_company_lock", "otc.company.margin.release", "场外商户保证金解冻"),
    OTC_COMPANY_LOCK("otc_company_lock", "otc.company.lock", "场外商户保证金冻结"),

    TRANSFER_COMMON("transfer_common", "", "通用转账"),
    RETURN("return", "fund.transaction.scene.coin.return", "返还"),
    INVITE_RETURN("invite_return", "fund.transaction.scene.coin.invite_return", "邀请返还"),
    DIVIDEND("dividend", "fund.transaction.scene.coin.dividend", "分红"),
    DIVIDEND_COLLECTION("dividend_collection", "fund.transaction.scene.coin.dividend_collection", "分红手续费归集"),

    BRANCH_BONUS("branch_bonus", "", "经纪人下属交易手续费返利"),
    LOCK_POSITION("lock_position", "fund.transaction.scene.coin.lock.position", "锁仓账户"),//包含私募冻结和解冻

    DEPOSIT_TRANSFER("deposit_transfer", "deposit.transfer", "储值资金划转"),
    DEPOSIT_LOCK_IN("deposit_lock_in", "deposit.lock", "用户储值锁仓资金入金"),
    DEPOSIT_LOCK_OUT("deposit_lock_out", "deposit.lock.out", "用户储值锁仓资金解锁"),
    DEPOSIT_ACC_OUT("deposit_acc_out", "deposit.acc.out", "用户储值本金返还"),

    GRANT_COIN("grant_coin", "fund.transaction.scene.grant_coin", "发放代币锁仓"),
    THAW_COIN("thaw_coin", "fund.transaction.scene.thaw_coin", "解冻发放代币锁仓"),
    HOLDING_DIVIDEND("holding_dividend", "fund.transaction.scene.coin.holding.dividend", "持仓奖励"),

    CUSTOMIZED_DIVIDEND_COLLECTION("customized_dividend_collection", "fund.transaction.scene.coin.customized_dividend_collection", "定制分红手续费归集"),
    RECOVERY_COIN("recovery_coin", "fund.transaction.scene.recovery_coin", "回收代币"),
    NEWCOIN_PURCHASE("newcoin_purchase", "fund.transaction.scene.newcoin.purchase", "新币申购"),
    NEWCOIN_GIVE_OUT("newcoin_give_out", "fund.transaction.scene.newcoin.giveout", "新币发放"),
    NEWCOIN_BACK_OUT("newcoin_back_out", "fund.transaction.scene.newcoin.backout", "新币申购退还"),
    NEWCOIN_PURCHASE_REMIT("newcoin_purchase_remit", "fund.transaction.scene.newcoin.remit", "新币申购划账"),
    LOCK_POSITION_V2("lock_position_v2", "fund.transaction.scene.lock_position_v2", "用户余额锁仓"),
    S_UNLOCK_POSITION_V2("s_unlock_position_v2", "fund.transaction.scene.s_unlock_position_v2", "系统锁仓赠币"),
    UNLOCK_POSITION_V2("unlock_position_v2", "fund.transaction.scene.unlock_position_v2", "用户锁仓解锁"),
    INCREMENT_PROJECT_APPLY("increment_lock_position", "fund.transaction.scene.increment.project.apply", "理财锁仓"),
    INCREMENT_PROJECT_GAIN("increment_project_gain", "fund.transaction.scene.increment.project.gain", "理财利息结算"),
    INCREMENT_PROJECT_RETURN("increment_project_return", "fund.transaction.scene.increment.project.return", "理财本金释放"),


    APPREC_DIRECT_REWARD("apprec_direct_reward", "apprec.direct.reward", "直推奖励划账"),
    APPREC_TEAM_REWARD("apprec_team_reward", "apprec.team.reward", "团队奖励划账"),
    APPREC_WEIGHT_SHARE_REWARD("apprec_weight_share_reward", "apprec.weight.share.reward", "加权分红奖励划账"),
    APPREC_PRODUCT_TOKEN_REWARD("apprec_product_token_reward", "apprec.product.token.reward", "增值包产币划账"),

    APPREC_PACKAGE_ORDER("apprec_package_order", "apprec.package.order", "增值包购买划账"),
    APPREC_TOKEN_ORDER("apprec_token_order", "apprec.token.order", "代币购买划账"),
    APPREC_PRODUCT_TOKEN_RECORD("apprec_product_token_record", "apprec.product.token.record", "领取增值包产币划账"),

    /**
     * 合约账户转入钱包账户
     */
    CONTRACT_TO_WALLET("contract_to_wallet", "scene.contract.to.wallet", "合约账户转入钱包账户"),
    /**
     * 钱包账户 转入合约账户
     */
    WALLET_TO_CONTRACT("wallet_to_contract", "scene.wallet.to.contract", "钱包账户转入合约账户"),


    //opay 支付平台部分的场景
    OP_ACCOUNT_INFLOWS("op_account_inflows", "op.account.inflows", "非支付场景，资金流水中的入账，涵盖多个条件，如：收入，收到退款等。"),
    OP_ACCOUNT_OUTFLOWS("op_account_outflows", "op.account.outflows", "非支付场景，资金流水中的出账，涵盖多个条件，如：支付，退款给指定用户等。"),
    OP_COIN_PAY_TRANSFER("op_coin_pay_transfer", "op.coin.pay.transfer", "币币账户到支付可用划转"),
    OP_PAY_COIN_TRANSFER("op_pay_coin_transfer", "op.pay.coin.transfer", "支付可用到币币账户划转"),
    OP_PAYEE_FREEZE_TO_PAY_CLEARING("op_payee_freeze_to_pay_clearing", "op.payee.freeze.to.pay.clearing", "商家冻结账户到支付可用结算"),
    OP_PAY_TO_PAYEE_FREEZE_PAYMENT("op_pay_to_payee_freeze_payment", "op.pay.to.payee.freeze.payment", "支付可用到商家冻结账户的支付"),
    OP_REFUND_APP_TRANSFER_USER("op_app_transfer_user_refund", "op.app.transfer.user.refund", "商家币币可用到用户币币可用的退款操作"),
    //冻结至钱包
    LOCK_TO_WALLET("lock_to_wallet", "scene.lock.to.wallet", "冻结账户转入钱包账户"),

    //杠杆交易场景
    LEVER_TRANSFER("lever_transfer", "lever.transfer", "杠杆资金划转"),
    LEVER_BORROW("lever_borrow", "lever.borrow", "借贷"),
    LEVER_RETURN_BORROW("lever_return_borrow", "lever.return.borrow", "归还本金"),
    LEVER_RETURN_INTREST("lever_return_intrest", "lever.return.intrest", "归还利息"),
    LEVER_DAY_INTREST("lever_day_intrest", "lever.day.intrest", "每日利息"),

    TRANSFER_OPERATE("transfer_operate", "transfer_operate", "调账流水"),

    //free stacking 类型
    INCOME_RECEIPT("free_staking_income_receipt", "free.staking.income.receipt", "收益到账"),
    PRINCIPAL_RELEASE("free_staking_principal_release", "free.staking.principal.release", "本金释放"),
    PRINCIPAL_LOCK("free_staking_principal_lock", "free.staking.principal.lock", "本金锁仓"),


    //快捷买币 私有化流动性saas广告交易场景
    OTC_FLOW_BUY_PAY("otc_flow_buy_pay", "otc_flow_buy_pay", "流动性订单买单确认放币"),//公司流动性出金账户=》用户场外余额账户
    OTC_FLOW_SELL_ORDER("otc_flow_sell_order", "otc_flow_sell_order", "流动性订单卖单下单冻结"),//用户场外余额=》用户流动性冻结账户
    OTC_FLOW_SELL_PAY("otc_flow_sell_pay", "otc_flow_sell_pay", "流动性订单卖单确认放币"),//用户流动性冻结账户=》公司流动性入金账户
    OTC_FLOW_SELL_CANCEL("otc_flow_sell_cancel", "otc_flow_sell_cancel", "流动性订单卖单取消订单"),//用户流动性冻结账户=》用户场外余额

    // -------------------------------------------------------------
    // Bitcheck scenes
    //

    /**
     * How to tag the scence for a bitcheck operation.
     */
    BITCHECK_TRANSFER("bitcheck", "bitcheck.transfer", "BitCheck transfer by user"),
    MYXCHANGE_TRANSFER_FEE("myxchange_transfer_fee", "myxchange.transfer.fee", "Transfer MyXchange fee by user"),
    BITCHECK_SYSTEM_TRANSFER("bitcheck_system_transfer", "bitcheck.system.transfer", "Transfer BitCheck balance by system"),
    MYXCHANGE_SYSTEM_TRANSFER_FEE("myxchange_system_transfer_fee", "myxchange.system.transfer.fee", "Transfer MyXchange fee by system"),
    REFERRAL_CODE_TRADE_REVENUE("referral_code_trade_revenue", "revenue.code.trade.revenue", "Referral code trade revenue"),
    REFERRAL_CODE_MYXCHANGE_TRADE_REVENUE("referral_code_myxchange_trade_revenue", "revenue.code.myxchange.trade.revenue", "Referral code MyXchange trade revenue"),

    /**
     * DeFi Stake
     */
    DEFI_STAKING("defi_staking_transfer", "defi.staking.transfer", "Transfer DeFi staking by user"),
    DEFI_STAKED("defi_staked_transfer", "defi.staked.transfer", "Transfer DeFi staked by system"),
    DEFI_STAKE_FEE("defi_stake_transfer_fee", "defi.stake.transfer.fee", "Transfer DeFi stake fee by user"),
    DEFI_REDEEM("defi_redeem_transfer", "defi.redeem.transfer", "Transfer Transfer DeFi redeem by user"),
    DEFI_REDEEM_FEE("defi_redeem_transfer_fee", "defi.redeem.transfer.fee", "Transfer DeFi redeem fee by user"),
    DEFI_REWARD("defi_reward", "defi.reward", "Transfer DeFi Reward by system"),
    DEFI_XREX_STAKE("defi_xrex_stake_transfer", "defi.xrex.stake.transfer", "Transfer DeFi stake by XREX"),
    DEFI_XREX_REDEEM("defi_xrex_redeem_transfer", "defi.xrex.redeem.transfer", "Transfer DeFi stake by XREX"),
    DEFI_XREX_REVENUE("defi_xrex_revenue_transfer", "defi.xrex.revenue.transfer", "Transfer DeFi revenue by XREX"),

    /**
     * AUM Stake
     */
    AUM_STAKING("aum_staking_transfer", "aum.staking.transfer", "Transfer AUM staking by user"),
    AUM_STAKED("aum_staked_transfer", "aum.staked.transfer", "Transfer AUM staked by system"),
    AUM_STAKE_FEE("aum_stake_transfer_fee", "aum.stake.transfer.fee", "Transfer AUM stake fee by user"),
    AUM_REDEEM("aum_redeem_transfer", "aum.redeem.transfer", "Transfer Transfer AUM redeem by user"),
    AUM_REDEEM_FEE("aum_redeem_transfer_fee", "aum.redeem.transfer.fee", "Transfer AUM redeem fee by user"),
    AUM_USER_REWARD("aum_user_reward", "aum.user.reward", "Transfer AUM Reward to user by system"),
    AUM_CLUB_REWARD("aum_club_reward", "aum.club.reward", "Transfer AUM Reward to club owner by system"),
    AUM_REWARD_MANAGEMENT_FEE("aum_reward_management_fee", "aum.reward.management.fee", "Transfer AUM user partial reward to XREX"),

    CONVERT("convert", "convert", "兑换"),

    /**
     * Margin Trade
     */
    MARGIN_INIT_COLLATERAL("margin_init_collateral_transfer", "margin.init_collateral.transfer", "Put initial collateral by user"),
    MARGIN_RETURN_COLLATERAL("margin_return_collateral_transfer", "margin.return_collateral.transfer", "Return collateral to user"),
    MARGIN_INCREASE_COLLATERAL("margin_increase_collateral_transfer", "margin.increase_collateral.transfer", "Increase collateral by user"),
    MARGIN_DECREASE_COLLATERAL("margin_decrease_collateral_transfer", "margin.decrease_collateral.transfer", "Decrease collateral by user"),

    MARGIN_TAKE_LOAN("margin_take_loan_transfer", "margin.take_loan.transfer", "Take a loan by user"),
    MARGIN_REPAY_LOAN("margin_repay_loan_transfer", "margin.repay_loan.transfer", "Repay a loan by user"),
    MARGIN_FUNDING_FEE("margin_funding_fee_transfer","margin.funding_fee.transfer", "Charge funding fee from user"),
    MARGIN_TRADING_FEE("margin_trading_fee_transfer","margin.trading_fee.transfer", "Charge trading fee from user"),

    /**
     * Grid Trade
     */
    GRID_TRADING_FEE("grid_trading_fee_transfer","grid.trading_fee.transfer", "Charge trading fee from user"),

    /**
     * Grid Margin Trade
     */
    GRID_MARGIN_TRADING_FEE("grid_margin_trading_fee_transfer","grid.margin.trading_fee.transfer", "Charge trading fee from user"),

    ;

    public String value;
    public String languageKey;
    public String description;

    private TransactionScene(String value, String languageKey, String description) {
        this.value = value;
        this.languageKey = languageKey;
        this.description = description;
    }

    public String getLanguageKey() {
        return languageKey;
    }

    public void setLanguageKey(String languageKey) {
        this.languageKey = languageKey;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public static TransactionScene fromValue(String value) {
        for (TransactionScene t : TransactionScene.values()) {
            if (t.value.equalsIgnoreCase(value)) {
                return t;
            }
        }
        return null;
    }

    public static TransactionScene fromName(String name) {
        for (TransactionScene t : TransactionScene.values()) {
            if (t.name().equals(name)) {
                return t;
            }
        }
        return null;
    }

}