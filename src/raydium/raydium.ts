/*
  文件概览：Raydium SDK V2 的核心入口与上下文容器。

  作用：
  - 统一管理链上连接、用户身份（owner）、日志、API 客户端等全局资源。
  - 聚合并实例化各业务模块（账户、流动性、CLMM/CPMM、交易、代币、Launchpad、Farm、IDO、市场等）。
  - 对外提供常用系统能力：链上时间/偏移量、纪元信息、功能可用性、代币列表缓存等。
  - 提供便捷方法设置/切换 connection、owner、批量签名器等。

  典型用法：
    const raydium = await Raydium.load({ connection, cluster, owner, ... });
    // 之后通过 raydium.clmm / raydium.tradeV2 / raydium.liquidity 等模块完成业务操作。

  重要设计：
  - 轻量缓存：API 返回的 token 列表、Jupiter 列表、链上时间与 epoch 信息等均带缓存时间，避免频繁请求。
  - 可用性检查：从 API 拉取全局/细分功能的可用状态，用于在应用中动态开关功能。
*/

import { Connection, Keypair, PublicKey, EpochInfo, Commitment } from "@solana/web3.js";
import { merge } from "lodash";

import { Api, API_URL_CONFIG, ApiV3TokenRes, ApiV3Token, JupTokenType, AvailabilityCheckAPI3 } from "../api";
import { EMPTY_CONNECTION, EMPTY_OWNER } from "../common/error";
import { createLogger, Logger } from "../common/logger";
import { Owner } from "../common/owner";
import { Cluster } from "../solana";

import Account, { TokenAccountDataProp } from "./account/account";
import Farm from "./farm/farm";
import Liquidity from "./liquidity/liquidity";
import { Clmm } from "./clmm";
import Cpmm from "./cpmm/cpmm";
import TradeV2 from "./tradeV2/trade";
import Utils1216 from "./utils1216";
import MarketV2 from "./marketV2";
import Ido from "./ido";
import Launchpad from "./launchpad/launchpad";

import TokenModule from "./token/token";
import { SignAllTransactions } from "./type";

/**
 * Raydium 初始化参数（高频外部入参）。
 * - 包含链上连接、网络、用户、公链时间缓存、批量签名器与 API 行为配置等。
 */
export interface RaydiumLoadParams extends TokenAccountDataProp, Omit<RaydiumApiBatchRequestParams, "api"> {
  /* ================= solana ================= */
  // solana web3 connection
  connection: Connection;
  // solana cluster/network/env
  cluster?: Cluster;
  // user public key
  owner?: PublicKey | Keypair;
  /* ================= api ================= */
  // api request interval in ms, -1 means never request again, 0 means always use fresh data, default is 5 mins (5 * 60 * 1000)
  apiRequestInterval?: number;
  // api request timeout in ms, default is 10 secs (10 * 1000)
  apiRequestTimeout?: number;
  apiCacheTime?: number;
  signAllTransactions?: SignAllTransactions;
  urlConfigs?: API_URL_CONFIG;
  logRequests?: boolean;
  logCount?: number;
  jupTokenType?: JupTokenType;
  disableFeatureCheck?: boolean;
  disableLoadToken?: boolean;
  blockhashCommitment?: Commitment;
  loopMultiTxStatus?: boolean;
}

export interface RaydiumApiBatchRequestParams {
  api: Api;
  defaultChainTimeOffset?: number;
  defaultChainTime?: number;
}

export type RaydiumConstructorParams = Required<RaydiumLoadParams> & RaydiumApiBatchRequestParams;

interface DataBase<T> {
  fetched: number;
  data: T;
  extInfo?: Record<string, any>;
}
interface ApiData {
  tokens?: DataBase<ApiV3Token[]>;

  // v3 data
  tokenList?: DataBase<ApiV3TokenRes>;
  jupTokenList?: DataBase<ApiV3Token[]>;
}

/**
 * Raydium 主类：聚合各业务模块并维护全局上下文与缓存。
 *
 * 职责：
 * - 通过构造函数注入 `connection`、`cluster`、`api` 等依赖
 * - 实例化模块：`account`、`liquidity`、`clmm`、`cpmm`、`tradeV2`、`token`、`farm`、`launchpad`、`marketV2`、`ido` 等
 * - 提供公共数据与能力：链上时间/偏移、纪元信息、功能可用性检查、代币列表（Raydium/Jupiter）缓存
 * - 统一 owner/connection/signAllTransactions 的设置与切换
 */
export class Raydium {
  public cluster: Cluster;
  public farm: Farm;
  public account: Account;
  public liquidity: Liquidity;
  public clmm: Clmm;
  public cpmm: Cpmm;
  public tradeV2: TradeV2;
  public utils1216: Utils1216;
  public marketV2: MarketV2;
  public ido: Ido;
  public token: TokenModule;
  public launchpad: Launchpad;
  public rawBalances: Map<string, string> = new Map();
  public apiData: ApiData;
  public availability: Partial<AvailabilityCheckAPI3>;
  public blockhashCommitment: Commitment;
  public loopMultiTxStatus?: boolean;

  private _connection: Connection;
  private _owner: Owner | undefined;
  public api: Api;
  private _apiCacheTime: number;
  private _signAllTransactions?: SignAllTransactions;
  private logger: Logger;
  private _chainTime?: {
    fetched: number;
    value: {
      chainTime: number;
      offset: number;
    };
  };
  private _epochInfo?: {
    fetched: number;
    value: EpochInfo;
  };

  /**
   * 构造函数：注入依赖并实例化所有子模块。
   */
  constructor(config: RaydiumConstructorParams) {
    const {
      connection,
      cluster,
      owner,
      api,
      defaultChainTime,
      defaultChainTimeOffset,
      apiCacheTime,
      blockhashCommitment = "confirmed",
      loopMultiTxStatus,
    } = config;

    this._connection = connection;
    this.cluster = cluster || "mainnet";
    this._owner = owner ? new Owner(owner) : undefined;
    this._signAllTransactions = config.signAllTransactions;
    this.blockhashCommitment = blockhashCommitment;
    this.loopMultiTxStatus = loopMultiTxStatus;

    this.api = api;
    this._apiCacheTime = apiCacheTime || 5 * 60 * 1000;
    this.logger = createLogger("Raydium");
    this.farm = new Farm({ scope: this, moduleName: "Raydium_Farm" });
    this.account = new Account({
      scope: this,
      moduleName: "Raydium_Account",
      tokenAccounts: config.tokenAccounts,
      tokenAccountRawInfos: config.tokenAccountRawInfos,
    });
    // 标准池（AMM）与稳定池的增删查等能力
    this.liquidity = new Liquidity({ scope: this, moduleName: "Raydium_LiquidityV2" });
    // 代币模块：加载代币列表、元信息解析与工具函数
    this.token = new TokenModule({ scope: this, moduleName: "Raydium_tokenV2" });
    // 路由和交易撮合（V2）
    this.tradeV2 = new TradeV2({ scope: this, moduleName: "Raydium_tradeV2" });
    // 集中流动性做市（CLMM）模块
    this.clmm = new Clmm({ scope: this, moduleName: "Raydium_clmm" });
    // 恒定乘积做市（CPMM）模块
    this.cpmm = new Cpmm({ scope: this, moduleName: "Raydium_cpmm" });
    // 常用工具集合（签名、交易、辅助计算等）
    this.utils1216 = new Utils1216({ scope: this, moduleName: "Raydium_utils1216" });
    // 市场（如订单簿创建等）
    this.marketV2 = new MarketV2({ scope: this, moduleName: "Raydium_marketV2" });
    // Ido/Launchpad 场景相关能力
    this.ido = new Ido({ scope: this, moduleName: "Raydium_ido" });
    this.launchpad = new Launchpad({ scope: this, moduleName: "Raydium_lauchpad" });

    this.availability = {};
    const now = new Date().getTime();
    this.apiData = {};

    if (defaultChainTimeOffset)
      this._chainTime = {
        fetched: now,
        value: {
          chainTime: defaultChainTime || Date.now() - defaultChainTimeOffset,
          offset: defaultChainTimeOffset,
        },
      };
  }

  /**
   * 一站式创建并初始化 Raydium 实例：
   * - 构建内部 API 客户端
   * - 预拉取功能可用性
   * - 可选：预加载 token 列表（含 Jupiter）
   */
  static async load(config: RaydiumLoadParams): Promise<Raydium> {
    const custom: Required<RaydiumLoadParams> = merge(
      // default
      {
        cluster: "mainnet",
        owner: null,
        apiRequestInterval: 5 * 60 * 1000,
        apiRequestTimeout: 10 * 1000,
      },
      config,
    );
    const { cluster, apiRequestTimeout, logCount, logRequests, urlConfigs } = custom;

    const api = new Api({ cluster, timeout: apiRequestTimeout, urlConfigs, logCount, logRequests });
    const raydium = new Raydium({
      ...custom,
      api,
    });

    await raydium.fetchAvailabilityStatus(config.disableFeatureCheck ?? true);
    if (!config.disableLoadToken)
      await raydium.token.load({
        type: config.jupTokenType,
      });

    return raydium;
  }

  get owner(): Owner | undefined {
    return this._owner;
  }
  get ownerPubKey(): PublicKey {
    if (!this._owner) throw new Error(EMPTY_OWNER);
    return this._owner.publicKey;
  }
  public setOwner(owner?: PublicKey | Keypair): Raydium {
    this._owner = owner ? new Owner(owner) : undefined;
    this.account.resetTokenAccounts();
    return this;
  }
  get connection(): Connection {
    if (!this._connection) throw new Error(EMPTY_CONNECTION);
    return this._connection;
  }
  public setConnection(connection: Connection): Raydium {
    this._connection = connection;
    return this;
  }
  get signAllTransactions(): SignAllTransactions | undefined {
    return this._signAllTransactions;
  }
  public setSignAllTransactions(signAllTransactions?: SignAllTransactions): Raydium {
    this._signAllTransactions = signAllTransactions;
    return this;
  }

  /**
   * 校验是否已设置 owner。未设置时抛错。
   */
  public checkOwner(): void {
    if (!this.owner) {
      console.error(EMPTY_OWNER);
      throw new Error(EMPTY_OWNER);
    }
  }

  /**
   * 判断缓存是否失效（基于 `_apiCacheTime`）。
   */
  private isCacheInvalidate(time: number): boolean {
    return new Date().getTime() - time > this._apiCacheTime;
  }

  /**
   * 从 API 获取链上时间偏移量并缓存。
   */
  public async fetchChainTime(): Promise<void> {
    try {
      const data = await this.api.getChainTimeOffset();
      this._chainTime = {
        fetched: Date.now(),
        value: {
          chainTime: Date.now() + data.offset * 1000,
          offset: data.offset * 1000,
        },
      };
    } catch {
      this._chainTime = undefined;
    }
  }

  /**
   * 获取 Raydium Token 列表（带缓存，可通过 `forceUpdate` 强制刷新）。
   */
  public async fetchV3TokenList(forceUpdate?: boolean): Promise<ApiV3TokenRes> {
    if (this.apiData.tokenList && !this.isCacheInvalidate(this.apiData.tokenList.fetched) && !forceUpdate)
      return this.apiData.tokenList.data;
    try {
      const raydiumList = await this.api.getTokenList();
      const dataObject = {
        fetched: Date.now(),
        data: raydiumList,
      };
      this.apiData.tokenList = dataObject;

      return dataObject.data;
    } catch (e) {
      console.error(e);
      return {
        mintList: [],
        blacklist: [],
        whiteList: [],
      };
    }
  }

  /**
   * 获取 Jupiter Token 列表（带缓存，可通过 `forceUpdate` 强制刷新）。
   */
  public async fetchJupTokenList(forceUpdate?: boolean): Promise<ApiV3Token[]> {
    if (this.cluster === "devnet") return [];
    const prevFetched = this.apiData.jupTokenList;
    if (prevFetched && !this.isCacheInvalidate(prevFetched.fetched) && !forceUpdate) return prevFetched.data;
    try {
      const jupList = await this.api.getJupTokenList();

      this.apiData.jupTokenList = {
        fetched: Date.now(),
        data: jupList.map((t) => ({
          ...t,
          mintAuthority: t.mint_authority || undefined,
          freezeAuthority: t.freeze_authority || undefined,
        })),
      };

      return this.apiData.jupTokenList.data;
    } catch (e) {
      console.error(e);
      return [];
    }
  }

  get chainTimeData(): { offset: number; chainTime: number } | undefined {
    return this._chainTime?.value;
  }

  /**
   * 返回链上时间偏移量（毫秒）。缓存 5 分钟。
   */
  public async chainTimeOffset(): Promise<number> {
    if (this._chainTime && Date.now() - this._chainTime.fetched <= 1000 * 60 * 5) return this._chainTime.value.offset;
    await this.fetchChainTime();
    return this._chainTime?.value.offset || 0;
  }

  /**
   * 返回当前链上时间（毫秒）。缓存 5 分钟。
   */
  public async currentBlockChainTime(): Promise<number> {
    if (this._chainTime && Date.now() - this._chainTime.fetched <= 1000 * 60 * 5)
      return this._chainTime.value.chainTime;
    await this.fetchChainTime();
    return this._chainTime?.value.chainTime || Date.now();
  }

  /**
   * 获取 Solana 纪元信息（缓存 30 秒）。
   */
  public async fetchEpochInfo(): Promise<EpochInfo> {
    if (this._epochInfo && Date.now() - this._epochInfo.fetched <= 1000 * 30) return this._epochInfo.value;
    this._epochInfo = {
      fetched: Date.now(),
      value: await this.connection.getEpochInfo(),
    };
    return this._epochInfo.value;
  }

  /**
   * 从 API 拉取功能可用性。
   * - 若 `skipCheck` 为 true，返回空对象并跳过请求。
   * - 若 `all` 为 false，则细分功能均视为不可用。
   */
  public async fetchAvailabilityStatus(skipCheck?: boolean): Promise<Partial<AvailabilityCheckAPI3>> {
    if (skipCheck) return {};
    try {
      const data = await this.api.fetchAvailabilityStatus();
      const isAllDisabled = data.all === false;
      this.availability = {
        all: data.all,
        swap: isAllDisabled ? false : data.swap,
        createConcentratedPosition: isAllDisabled ? false : data.createConcentratedPosition,
        addConcentratedPosition: isAllDisabled ? false : data.addConcentratedPosition,
        addStandardPosition: isAllDisabled ? false : data.addStandardPosition,
        removeConcentratedPosition: isAllDisabled ? false : data.removeConcentratedPosition,
        removeStandardPosition: isAllDisabled ? false : data.removeStandardPosition,
        addFarm: isAllDisabled ? false : data.addFarm,
        removeFarm: isAllDisabled ? false : data.removeFarm,
      };
      return data;
    } catch {
      return {};
    }
  }
}
