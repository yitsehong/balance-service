package io.xrex.enums;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public enum AssetType_A_BC {

	// 公司账户类型
	/** 公司：交易手续费 */
	C_TRADE_FEE("101", "1", "01", "公司：交易手续费"),
	/** 公司：充值 */
	C_DEPOSIT("102", "1", "02", "公司：充值"),
	/** 公司提现账户 */
	C_WITHDRAW("103", "1", "03", "公司：已提现"),
	/** 公司：运营平账 */  //最开始想做成运营调账, 调账工具那里的, 但是后来调账工具做的太自由,且只能个人之见互转,这个账户完全废弃了
	C_OPERATE("104", "1", "04", "公司：运营平账"),
	/** 收用户的提现手续费 */
	C_WITHDRAW_FEE("105", "1", "05", "公司：提现手续费"),
	/** 实际消耗的手续费 区块链转账消耗提现手续费 */
	C_EXPEND_WITHDRAW_FEE("106", "1", "06", "公司：消耗提现手续费"),//矿工费
	/** 公司:场外交易手续费 */
	C_OTC_FEE("107", "1", "07", "公司:场外交易手续费"),
	/** 公司: 合约交易手续费账户*/
	C_CONTRACT_FEE("108", "1", "08", "公司:合约交易手续费账户"),

	/** 公司风险准备金账户*/
	C_RISKASSURE("109","1","09","公司:合约风险准备金账户"),
	/** 合约交割手续费账户*/
	C_DELIVERY_FEE("110","1","10","公司：合约交割手续费"),
	/**合约交易平仓接管账户*/
	C_POSITION_CLOSEE("111", "1", "11", "公司:合约交易平仓接管账户"),
	/** 公司:赠币账户 */
	C_PRESENT_COIN("112", "1", "12", "公司:赠币账户"),
	/** 公司:管理员充值账户*/
	C_COMPANY_DEPOSIT("113","1","13","公司:管理员充值账户"),
	/** 公司:分红币返还账户*/
	C_TRADE_FEE_REFUND("114","1","14","公司:分红币返还账户"), //挖矿
	/** 公司:分红币分红账户*/
	C_TRADE_FEE_DIVIDEND("115","1","15","公司:分红币分红账户"),
	/** 公司:锁仓账户 **/
	C_LOCKED_POSITION("116","1","16","公司:锁仓账户"),//公司私募账户
	/** 公司:储值锁仓账户 **/
	C_DEPOSIT_LOCKED("117","1","17","公司:储值锁仓账户"),
	/** 邀请人返利奖金账户**/
	C_BRANCH_BONUS("118","1","18","公司:邀请人返利奖金账户"),
	/**合约资金费用账户*/
	C_CAPITAL_COST("119", "1", "19", "公司:合约资金费用账户"),
	/** 公司发放代币账户**/
	C_GRANT_COIN("120","1","20","公司:公司发放代币账户"),

	/** 公司:冷钱包 **/
	C_COLD_WALLET_BALANCE("100","1", "00","冷钱包"),
	/** 公司:热钱包 **/
	C_HOT_WALLET_BALANCE("199","1", "99","热钱包"),

	/**
	 * 公司:申购账户
	 **/
	C_NC_DEPOSIT("121", "1", "21", "公司:申购账户"),
	/**
	 * 公司:发币账户
	 **/
	C_NC_FEE_REFUND("122", "1", "22", "公司:发币账户"),
	/**
	 * 公司持仓奖励账户
	 **/
	C_HOLD_DIVIDEND("123", "1", "23", "公司:公司持仓奖励账户"),

	/**
	 * 公司发放代币账户
	 **/
	C_UNLOCK_POSITION_V2("124", "1", "24", "公司:公司发放锁仓代币账户V2"),

	/**
	 * 公司流动性场外入金账户  流动性订单用户给此账户转账
	 **/
	C_OTC_FLOW_DEPOSIT("125", "1", "25", "公司:公司流动性场外入金账户"),
	/**
	 * 公司流动性场外出金账户 流动性订单从此账户给用户转账
	 **/
	C_OTC_FLOW_WITHDRAW("126", "1", "26", "公司:公司流动性场外出金账户"),

	// 用户账户类型
	/**
	 * 用户现货正常余额
	 */
	U_NORMAL("201", "2", "01", "用户:正常余额"),
	/**
	 * 用户现货冻结余额
	 */
	U_LOCK("202", "2", "02", "用户:冻结"),
	/**
	 * 用户提现中余额
	 */
	U_WITHDRAW("203", "2", "03", "用户:提现中"),
	/**
	 * 用户赠币账户余额
	 */
	U_PRESENT_COIN("206", "2", "06", "用户:赠币账户"),
	/** 用户锁仓账户余额 */
	U_LOCKED_POSITION("207", "2", "07", "锁仓账户"),
	/** 用户储值锁仓账户余额 */
	U_DEPOSIT_LOCKED("208", "2", "08", "储值锁仓账户"),
	/** 用户储值账户余额 */
	U_DEPOSIT("209", "2", "09", "储值账户"),
	/** 用户场外商户保证金账户 */
	U_OTC_COMPANY_MARGIN("210", "2", "10", "场外商户保证金账户"),
	/**
	 * 保证金账户
	 */
	U_MARGIN("216", "2", "16", "合约保证金账户"),
	/**
	 * 用户场外正常余额
	 */
	U_OTC_NORMAL("217", "2", "17", "用户:场外正常余额"),
	/**
	 * 用户场外冻结余额
	 */
	U_OTC_LOCK("218", "2", "18", "用户:场外冻结"),
	/**
	 * 用户代币锁仓账户
	 */
	U_GRANT_COIN("220", "2", "20", "用户:代币锁仓账户"),

	/**
	 * 用户申购冻结余额
	 */
	U_NC_LOCK("221", "2", "21", "申购冻结"),
	/**
	 * 用户代币锁仓账户V2
	 */
	U_LOCK_POSITION_V2("222", "2", "22", "用户:代币锁仓账户V2"),
	/**
	 * 用户:理财申购账户(free staking)
	 */
	U_LOCK_INCREMENT("260", "2", "60", "用户:理财锁仓账户"),


	/**
	 * 开放平台 支付可用账户
	 */
	U_OPEN_PLATFORM_PAY("241", "2", "41", "开放平台 - 支付可用账户"),
	/**
	 * 开放平台 收款冻结账户
	 */
	U_OPEN_PLATFORM_PAYEE_LOCKED("242", "2", "42", "开放平台 - 收款冻结账户"),

	/**
	 * 流动性订单用户冻结账户
	 */
	U_OTC_FLOW_LOCK("243", "2", "43", "流动性订单用户冻结账户"),

	//杠杆账户
	U_LEVER_NORMAL("250", "2", "50", "用户:杠杆正常余额"),
	U_LEVER_LOCK("251", "2", "51", "用户:杠杆冻结"),
	U_LEVER_CURRENT_BORROW("252", "2", "52", "用户:杠杆应还借贷"),
	U_LEVER_CURRENT_INTEREST("253", "2", "53", "用户:杠杆应还利息账户"),

	/* 公司杠杆相关账户  应收本金，应收利息**/
	C_LEVER_TRADE_FEE("141","1","41","公司:杠杆交易手续费账户"),
	C_LEVER_LEND("142","1","42","公司:杠杆出借账户"),
	C_LEVER_COLLECTION("143","1","43","公司:杠杆收款账户"),
	C_LEVER_INTEREST("144","1","44","公司:杠杆利息账户"),
	C_LEVER_BORROW_TRANSACTION("145","1","45","公司:虚拟流水杠杆出借账户"),
	C_LEVER_INTEREST_TRANSACTION("146","1","46","公司:虚拟流水杠杆利息账户"),

	// ------------------------------------------------------
	// Bitcheck user level accounts
	//

	/**
	 * Bitcheck normal balances (freely available)
	 */
	U_BITCHECK_NORMAL("281", "2", "81", "Bitcheck available balance for a currency"),

	/**
	 * Bticheck escrowed balances (not available to the user)
	 */
	U_BITCHECK_ESCROW("282", "2", "82", "Bitcheck escrowed balance for a currency"),

	/**
	 * Currency that is locked from being used because it has been committed to a bitcheck
	 */
	U_BITCHECK_LOCKED("283", "2", "83", "Bitcheck locked balance awaiting delivery"),

	/**
	 * MyXchange operator fee accounts
	 */
	U_XCHANGE_FEE("284", "2", "84", "MyXChange operator fees"),

	/**
	 * Company level MyXchange trading fees
	 */
	C_XCHANGE_FEE("181", "1", "81", "MyXChange fees earned by the company"),

	/**
	 * Company level BitCheck Fees earned
	 */
	C_BITCHECK_FEE("182", "1", "82", "Bitcheck fees earned by the company" ),


	// ------------------------------------------------------
	// Defi Staking accounts
	//
	U_DEFI_STAKING_LOCKED("285", "2", "85", "User DeFi staking locked balance"),
	U_DEFI_STAKED_LOCKED("286", "2", "86", "User DeFi staked locked balance"),

	C_DEFI_STAKING_FEE("183", "1", "83", "DeFi staking fees earned by the XREX"),
	C_DEFI_REDEEM_FEE("184", "1", "84", "DeFi redeem fees earned by the XREX"),
	C_DEFI_STAKE("185", "1", "85", "XREX DeFi stake account balance"),
	C_DEFI_CONTRACT("186", "1", "86", "XREX DeFi contract account balance"),
	C_DEFI_REVENUE("187", "1", "87", "XREX DeFi revenue balance"),

	// ------------------------------------------------------
	// AUM Staking accounts
	//
	U_AUM_STAKING_LOCKED("287", "2", "87", "User AUM staking locked balance"),
	U_AUM_STAKED_LOCKED("288", "2", "88", "User AUM staked locked balance"),

	C_AUM_STAKING_FEE("188", "1", "88", "AUM staking fees earned by the XREX"),
	C_AUM_REDEEM_FEE("189", "1", "89", "AUM redeem fees earned by the XREX"),
	C_AUM_STAKE("190", "1", "90", "XREX AUM stake account balance"),
	C_AUM_REWARD_MANAGEMENT_FEE("191", "1", "91", "XREX AUM reward management fee earned by the XREX"),

	// ------------------------------------------------------
	// Grid trading bot accounts
	//
	U_GRID_NORMAL("291", "2", "91", "Grid bot trading normal account"),
	U_GRID_LOCK("292", "2", "92", "Grid bot trading lock account"),
	C_GRID_TRADING_FEE("192", "1", "92", "Grid trading fee"),

	// ------------------------------------------------------
	// Margin Trading Account
	//
	U_MARGIN_ISOLATED_EQUITY_LOCKED("295", "2", "95", "Margin isolated equity locked balance"),

	C_MARGIN_LENDING_POOL("1100", "1", "100", "Margin lending pool"),
	C_MARGIN_FUNDING_FEE("1101", "1", "101", "Margin funding fee"),
	C_MARGIN_TRADING_FEE("1102", "1", "102", "Margin trading fee"),

	// ------------------------------------------------------
	// Grid margin bot accounts
	//
	U_GRID_MARGIN_NORMAL("293", "2", "93", "Grid margin bot trading normal account"),
	U_GRID_MARGIN_LOCK("294", "2", "94", "Grid margin bot trading lock account"),
	U_GRID_MARGIN_COLLATERAL("296", "2", "96", "Grid margin bot free collateral"),
	C_GRID_MARGIN_TRADING_FEE("1122", "1", "122", "Grid margin bot trading fee"),
	C_GRID_MARGIN_COST("1123", "1", "123", "Grid margin bot margin cost"),

	// ------------------------------------------------------
	// Market Maker accounts
	U_MM_NORMAL("254", "2", "54", "Market making normal balance"),
	U_MM_CREDIT("255", "2", "55", "Market making credit balance"),
	C_MM_CREDIT("155", "1", "55", "Market making credit balance by the company")
	;

	// Define the related DeFi account types
	public final static AssetType_A_BC[] DEFI_STAKE_ACCOUNT_TYPES = new AssetType_A_BC[]{
			U_DEFI_STAKING_LOCKED,
			U_DEFI_STAKED_LOCKED,
			C_DEFI_STAKING_FEE,
			C_DEFI_REDEEM_FEE,
			C_DEFI_STAKE,
			C_DEFI_CONTRACT,
			C_DEFI_REVENUE
	};

	// Define the related DeFi account types
	public final static AssetType_A_BC[] AUM_STAKE_ACCOUNT_TYPES = new AssetType_A_BC[]{
			U_AUM_STAKING_LOCKED,
			U_AUM_STAKED_LOCKED,
			C_AUM_STAKING_FEE,
			C_AUM_REDEEM_FEE,
			C_AUM_STAKE,
			C_AUM_REWARD_MANAGEMENT_FEE
	};

	// Define the grid trading bot related account types
	public final static AssetType_A_BC[] GRID_ACCOUNT_TYPES = new AssetType_A_BC[]{
			U_GRID_NORMAL,
			U_GRID_LOCK,
			C_GRID_TRADING_FEE
	};

	// Define the related DeFi account types
	public final static AssetType_A_BC[] MARGIN_TRADE_ACCOUNT_TYPES = new AssetType_A_BC[]{
			U_MARGIN_ISOLATED_EQUITY_LOCKED,
			C_MARGIN_LENDING_POOL,
			C_MARGIN_FUNDING_FEE,
			C_MARGIN_TRADING_FEE
	};

	public final static AssetType_A_BC[] GRID_MARGIN_ACCOUNT_TYPES = new AssetType_A_BC[]{
			U_GRID_MARGIN_NORMAL,
			U_GRID_MARGIN_LOCK,
			U_GRID_MARGIN_COLLATERAL,
			C_GRID_MARGIN_TRADING_FEE,
			C_GRID_MARGIN_COST
	};

	public String	value;
	public String	account_A;
	public String	account_BC;
	public String	desc;

	AssetType_A_BC(String value, String account_A, String account_BC, String desc) {
		this.value = value;
		this.account_A = account_A;
		this.account_BC = account_BC;
		this.desc = desc;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getAccount_A() {
		return account_A;
	}

	public void setAccount_A(String account_A) {
		this.account_A = account_A;
	}

	public String getAccount_BC() {
		return account_BC;
	}

	public void setAccount_BC(String account_BC) {
		this.account_BC = account_BC;
	}

	public static AssetType_A_BC fromValue(String value) {
		for (AssetType_A_BC t : AssetType_A_BC.values()) {
			if (t.value.equalsIgnoreCase(value)) {
				return t;
			}
		}
		return null;
	}

	public static Boolean isFeeType(Integer assetType) {
		String abc = String.valueOf(assetType).substring(0, 3);
		List<String> feeTypeList = Arrays.asList(C_TRADE_FEE.value, C_LEVER_TRADE_FEE.value,
				C_MARGIN_FUNDING_FEE.value, C_MARGIN_TRADING_FEE.value, C_GRID_TRADING_FEE.value,
				C_GRID_MARGIN_TRADING_FEE.value);
		return feeTypeList.contains(abc);
	}

	/**
	 * 获取需要统计的账户枚举
	 * <br> values是够用了,但为方便以后扩展,所以这里还是专门写一个
	 *
	 * @return
	 * @author wangtao
	 * @Date 2018年7月18日 下午6:57:16
	 */
	public static AssetType_A_BC[] getStatsAssetType() {
		return new AssetType_A_BC[]{
				C_EXPEND_WITHDRAW_FEE, C_DEPOSIT, C_WITHDRAW,
				C_COMPANY_DEPOSIT, C_WITHDRAW_FEE, C_PRESENT_COIN, C_TRADE_FEE, C_OTC_FEE, C_TRADE_FEE_REFUND, C_BRANCH_BONUS, C_TRADE_FEE_DIVIDEND,
				U_NORMAL, U_LOCK, U_WITHDRAW, U_LOCKED_POSITION,
				U_OTC_NORMAL, U_OTC_LOCK, U_OTC_FLOW_LOCK, U_OTC_COMPANY_MARGIN,
				U_MARGIN, C_DELIVERY_FEE, C_CONTRACT_FEE, C_RISKASSURE,//合约需要统计的所有账户（保证金、交割手续费、交易手续费、风险准备金）
				C_OPERATE ,U_PRESENT_COIN,C_GRANT_COIN,U_GRANT_COIN,C_UNLOCK_POSITION_V2, U_LOCK_POSITION_V2, C_HOLD_DIVIDEND,
				C_OTC_FLOW_DEPOSIT, C_OTC_FLOW_WITHDRAW,//场外流动性公司账户出出入金账户
				C_XCHANGE_FEE, // MyExchange Fee
				C_AUM_STAKING_FEE, C_AUM_REDEEM_FEE, C_AUM_REWARD_MANAGEMENT_FEE,	// AUM
				C_MARGIN_FUNDING_FEE, C_MARGIN_TRADING_FEE	// Margin Trade

		};
	}

	/**
	 * 获取 属于资产的账户类型
	 *
	 * @return
	 * @author wangtao
	 * @Date 2018年7月19日 下午2:03:17
	 */
	public static AssetType_A_BC[] getAssets() {
		return new AssetType_A_BC[]{
				C_COMPANY_DEPOSIT, C_WITHDRAW_FEE, C_PRESENT_COIN, C_TRADE_FEE, C_OTC_FEE, C_TRADE_FEE_REFUND, C_BRANCH_BONUS,C_TRADE_FEE_DIVIDEND,C_LOCKED_POSITION,
				C_COLD_WALLET_BALANCE , C_HOT_WALLET_BALANCE,C_GRANT_COIN,C_NC_DEPOSIT,C_NC_FEE_REFUND,C_UNLOCK_POSITION_V2, C_HOLD_DIVIDEND,
				C_CONTRACT_FEE, C_DELIVERY_FEE, C_RISKASSURE, C_OTC_FLOW_WITHDRAW, C_OTC_FLOW_DEPOSIT,
				C_XCHANGE_FEE,	// MyExchange Fee
				C_AUM_STAKING_FEE, C_AUM_REDEEM_FEE, C_AUM_REWARD_MANAGEMENT_FEE,	// AUM
				C_MARGIN_FUNDING_FEE, C_MARGIN_TRADING_FEE 	// Margin Trade
		};
	}

	/**
	 * 获取 属于负债的账户类型
	 * @author wangtao
	 * @Date 2018年7月19日 下午2:03:17
	 * @return
	 */
	public static AssetType_A_BC[] getLiabilities() {
		return new AssetType_A_BC[] {
				U_NORMAL,U_LOCK,U_WITHDRAW,U_OTC_NORMAL,U_OTC_LOCK,U_LOCKED_POSITION,U_GRANT_COIN,U_NC_LOCK,U_LOCK_POSITION_V2,
				U_MARGIN,U_LOCK_INCREMENT,U_OTC_COMPANY_MARGIN
		};
	}

	public static AssetType_A_BC getEnumByValue(String value) {
		for (AssetType_A_BC t : AssetType_A_BC.values()) {
			if (t.value.equals(value)) {
				return t;
			}
		}
		return null;
	}

	/**
	 * 判断类型是资产,还是负债
	 * @author wangtao
	 * @Date 2018年7月19日 下午2:10:06
	 * @return
	 */
	public  boolean isCoType(){
		return this==C_CONTRACT_FEE || this==C_RISKASSURE || this == C_POSITION_CLOSEE || this ==C_DELIVERY_FEE || this == U_MARGIN || this == C_CAPITAL_COST;
	}

	/**
	 * 获取全部用户杠杆账户类型
	 * @return
	 */
	public  static List<String> getUserLeverType(){
		ArrayList<String> bcList = new ArrayList<>();
		bcList.add(U_LEVER_NORMAL.account_BC);
		bcList.add(U_LEVER_LOCK.account_BC);
		bcList.add(U_LEVER_CURRENT_BORROW.account_BC);
		bcList.add(U_LEVER_CURRENT_INTEREST.account_BC);
		return  bcList;
	}

	/**
	 * 获取杠杆账户
	 * @return
	 */
	public  static List<AssetType_A_BC> loadUserLeverType() {
		ArrayList<AssetType_A_BC> bcList = new ArrayList<>();
		bcList.add(U_LEVER_NORMAL);
		bcList.add(U_LEVER_LOCK);
		bcList.add(U_LEVER_CURRENT_BORROW);
		bcList.add(U_LEVER_CURRENT_INTEREST);
		return bcList;
	}

	/**
	 * 是否为用户杠杆账户
	 *
	 * @param abc
	 * @return
	 */
	public static boolean isUserLeverType(String abc) {
		boolean flag = false;
		if (U_LEVER_NORMAL.value.equals(abc) || U_LEVER_LOCK.value.equals(abc) || U_LEVER_CURRENT_BORROW.value.equals(abc)
				|| U_LEVER_CURRENT_INTEREST.value.equals(abc)) {
			flag = true;
		}
		return flag;
	}

	/**
	 * 是否为杠杆手续费账户
	 *
	 * @param abc
	 * @return
	 */
	public static boolean isLeverTradeFeeType(String abc) {
		boolean flag = false;
		if (C_LEVER_TRADE_FEE.value.equals(abc)) {
			flag = true;
		}
		return flag;
	}

	/**
	 * 是否为用户借贷杠杆账户
	 * @param abc
	 * @return
	 */
	public static  boolean isUserLeverBorrowAndInteresType(String abc){
		boolean flag = false;
		if (U_LEVER_CURRENT_BORROW.value.equals(abc) || U_LEVER_CURRENT_INTEREST.value.equals(abc)){
			flag = true;
		}
		return flag;
	}

	/**
	 * 是否为用户杠杆账户
	 * @param abc
	 * @return
	 */
	public static  boolean isUserLeverNormalAndLockType(String abc){
		boolean flag = false;
		if (U_LEVER_NORMAL.value.equals(abc) || U_LEVER_LOCK.value.equals(abc)){
			flag = true;
		}
		return flag;
	}

	public static AssetType_A_BC fromName(String name) {
		for (AssetType_A_BC t : AssetType_A_BC.values()) {
			if (t.name().equalsIgnoreCase(name)) {
				return t;
			}
		}
		return null;
	}

	public static  boolean isUserAndCLeverType(String abc){
		boolean flag = false;
		if (U_LEVER_NORMAL.value.equals(abc) || U_LEVER_LOCK.value.equals(abc) || U_LEVER_CURRENT_BORROW.value.equals(abc)
				|| U_LEVER_CURRENT_INTEREST.value.equals(abc)
				|| C_LEVER_TRADE_FEE.value.equals(abc)){
			flag = true;
		}
		return flag;
	}

	public static List<AssetType_A_BC> magnetoAucTypes() {
		return Arrays.asList(AssetType_A_BC.U_LOCK,
				AssetType_A_BC.U_NORMAL,
				AssetType_A_BC.U_DEPOSIT,
				AssetType_A_BC.U_NC_LOCK,
				AssetType_A_BC.U_WITHDRAW,
				AssetType_A_BC.U_DEPOSIT_LOCKED,
				AssetType_A_BC.U_LOCKED_POSITION,
				AssetType_A_BC.U_LOCK_INCREMENT,
				AssetType_A_BC.U_LOCK_POSITION_V2);
	}
}