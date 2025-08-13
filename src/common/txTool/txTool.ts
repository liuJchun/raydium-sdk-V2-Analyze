/*
  文件概览：交易构建与执行工具（TxBuilder）

  作用：
  - 为各业务模块（CLMM、Liquidity、TradeV2 等）提供统一的交易构建与执行能力。
  - 支持 Legacy 与 V0 版本交易的构建、分片（超长拆分）、签名与发送；支持多笔交易（批量或串行）执行。
  - 集成计算预算（compute budget）与小费（tip）指令的注入；根据网络费况自动估算预算费率。
  - 支持 Address Lookup Table（ALT）缓存与拼装，优化 V0 交易体积。

  关键特性：
  - addInstruction：按类型收集指令、尾指令、签名者、指令类型与 ALT 地址。
  - addCustomComputeBudget / calComputeBudget：手动或自动注入 compute budget。
  - addTipInstruction：添加小费转账指令。
  - build / buildV0 / versionBuild：返回可执行（execute）的构建结果。
  - buildMultiTx / buildV0MultiTx / sizeCheckBuild(V0)：按大小拆分或多笔组装并执行。
*/

import {
  Commitment,
  Connection,
  PublicKey,
  sendAndConfirmTransaction,
  SignatureResult,
  Signer,
  SystemProgram,
  Transaction,
  TransactionInstruction,
  TransactionMessage,
  VersionedTransaction,
} from "@solana/web3.js";
import axios from "axios";

import { Api } from "../../api";
import { ComputeBudgetConfig, SignAllTransactions, TxTipConfig } from "../../raydium/type";
import { Cluster } from "../../solana";
import { Owner } from "../owner";
import { CacheLTA, getDevLookupTableCache, getMultipleLookupTableInfo, LOOKUP_TABLE_CACHE } from "./lookupTable";
import { InstructionType, TxVersion } from "./txType";
import {
  addComputeBudget,
  checkLegacyTxSize,
  checkV0TxSize,
  confirmTransaction,
  getRecentBlockHash,
  printSimulate,
} from "./txUtils";

/**
 * Solana 网络费信息（来自 solanacompass）
 */
interface SolanaFeeInfo {
  min: number;
  max: number;
  avg: number;
  priorityTx: number;
  nonVotes: number;
  priorityRatio: number;
  avgCuPerBlock: number;
  blockspaceUsageRatio: number;
}
/**
 * 不同时间窗口（1/5/15 分钟）的费况数据
 */
type SolanaFeeInfoJson = {
  "1": SolanaFeeInfo;
  "5": SolanaFeeInfo;
  "15": SolanaFeeInfo;
};

/**
 * 交易执行参数
 * - skipPreflight：是否跳过模拟
 * - recentBlockHash：覆盖使用的区块哈希
 * - sendAndConfirm：是否在发送后等待确认
 * - notSendToRpc：仅返回已签名交易但不广播
 */
interface ExecuteParams {
  skipPreflight?: boolean;
  recentBlockHash?: string;
  sendAndConfirm?: boolean;
  notSendToRpc?: boolean;
}

/**
 * TxBuilder 初始化参数
 * - connection：RPC 连接
 * - feePayer：费用支付者
 * - cluster：网络环境（mainnet/devnet 等）
 * - owner：可选的用户（用于签名）
 * - blockhashCommitment：获取区块哈希时的承诺等级
 * - loopMultiTxStatus：多笔交易串行时是否轮询状态
 * - api：可选 API 客户端（保留扩展）
 * - signAllTransactions：批量签名函数（钱包适配）
 */
interface TxBuilderInit {
  connection: Connection;
  feePayer: PublicKey;
  cluster: Cluster;
  owner?: Owner;
  blockhashCommitment?: Commitment;
  loopMultiTxStatus?: boolean;
  api?: Api;
  signAllTransactions?: SignAllTransactions;
}

/**
 * 往 TxBuilder 中添加指令的数据结构
 * - instructions：普通指令
 * - endInstructions：尾部指令（如 close、tip）
 * - lookupTableAddress：V0 用到的 ALT 地址列表
 * - signers：额外签名者
 * - instructionTypes / endInstructionTypes：指令类型标记（仅用于记录/调试）
 */
export interface AddInstructionParam {
  addresses?: Record<string, PublicKey>;
  instructions?: TransactionInstruction[];
  endInstructions?: TransactionInstruction[];
  lookupTableAddress?: string[];
  signers?: Signer[];
  instructionTypes?: string[];
  endInstructionTypes?: string[];
}

/**
 * Legacy 交易构建结果
 */
export interface TxBuildData<T = Record<string, any>> {
  builder: TxBuilder;
  transaction: Transaction;
  instructionTypes: string[];
  signers: Signer[];
  execute: (params?: ExecuteParams) => Promise<{ txId: string; signedTx: Transaction }>;
  extInfo: T;
}

/**
 * V0 交易构建结果
 * - buildProps：包含 ALT 缓存与地址、forerunCreate 等构建上下文
 */
export interface TxV0BuildData<T = Record<string, any>> extends Omit<TxBuildData<T>, "transaction" | "execute"> {
  builder: TxBuilder;
  transaction: VersionedTransaction;
  buildProps?: {
    lookupTableCache?: CacheLTA;
    lookupTableAddress?: string[];
  };
  execute: (params?: ExecuteParams) => Promise<{ txId: string; signedTx: VersionedTransaction }>;
}

type TxUpdateParams = {
  txId: string;
  status: "success" | "error" | "sent";
  signedTx: Transaction | VersionedTransaction;
};
/**
 * 多交易执行参数
 * - sequentially：是否串行执行
 * - skipTxCount：跳过前 N 笔（已成功的场景）
 * - onTxUpdate：回调每笔交易状态（sent/success/error）
 */
export interface MultiTxExecuteParam extends ExecuteParams {
  sequentially: boolean;
  skipTxCount?: number;
  onTxUpdate?: (completeTxs: TxUpdateParams[]) => void;
}
/**
 * Legacy 批量交易构建结果
 */
export interface MultiTxBuildData<T = Record<string, any>> {
  builder: TxBuilder;
  transactions: Transaction[];
  instructionTypes: string[];
  signers: Signer[][];
  execute: (executeParams?: MultiTxExecuteParam) => Promise<{ txIds: string[]; signedTxs: Transaction[] }>;
  extInfo: T;
}

/**
 * V0 批量交易构建结果
 */
export interface MultiTxV0BuildData<T = Record<string, any>>
  extends Omit<MultiTxBuildData<T>, "transactions" | "execute"> {
  builder: TxBuilder;
  transactions: VersionedTransaction[];
  buildProps?: {
    lookupTableCache?: CacheLTA;
    lookupTableAddress?: string[];
  };
  execute: (executeParams?: MultiTxExecuteParam) => Promise<{ txIds: string[]; signedTxs: VersionedTransaction[] }>;
}

/** 条件类型：按版本返回 Legacy 或 V0 的批量交易类型 */
export type MakeMultiTxData<T = TxVersion.LEGACY, O = Record<string, any>> = T extends TxVersion.LEGACY
  ? MultiTxBuildData<O>
  : MultiTxV0BuildData<O>;

/** 条件类型：按版本返回 Legacy 或 V0 的单笔交易类型 */
export type MakeTxData<T = TxVersion.LEGACY, O = Record<string, any>> = T extends TxVersion.LEGACY
  ? TxBuildData<O>
  : TxV0BuildData<O>;

/** 多笔串行轮询间隔（毫秒） */
const LOOP_INTERVAL = 2000;

/**
 * 交易构建器：封装指令收集、预算/小费注入、签名与发送、V0/Legacy 构建与分片等能力。
 */
export class TxBuilder {
  /** RPC 连接 */
  private connection: Connection;
  /** 可选的 owner（若为 Keypair 可本地签名） */
  private owner?: Owner;
  /** 普通指令集合 */
  private instructions: TransactionInstruction[] = [];
  /** 尾部指令集合（如 close/tip） */
  private endInstructions: TransactionInstruction[] = [];
  /** ALT 地址集合（V0） */
  private lookupTableAddress: string[] = [];
  /** 额外签名者集合 */
  private signers: Signer[] = [];
  /** 指令类型标记（调试/记录） */
  private instructionTypes: string[] = [];
  /** 尾部指令类型标记（调试/记录） */
  private endInstructionTypes: string[] = [];
  /** 费用支付者 */
  private feePayer: PublicKey;
  /** 集群环境 */
  private cluster: Cluster;
  /** 批量签名器（适配钱包） */
  private signAllTransactions?: SignAllTransactions;
  /** 获取区块哈希时的承诺等级 */
  private blockhashCommitment?: Commitment;
  /** 多笔交易串行时，是否轮询交易状态（辅助前端展示） */
  private loopMultiTxStatus: boolean;

  /** 构造函数：注入连接、费支付者、owner、签名器等上下文 */
  constructor(params: TxBuilderInit) {
    this.connection = params.connection;
    this.feePayer = params.feePayer;
    this.signAllTransactions = params.signAllTransactions;
    this.owner = params.owner;
    this.cluster = params.cluster;
    this.blockhashCommitment = params.blockhashCommitment;
    this.loopMultiTxStatus = !!params.loopMultiTxStatus;
  }

  /** 返回已收集的所有指令、签名者与 ALT 地址（便于复用或外层拼装） */
  get AllTxData(): {
    instructions: TransactionInstruction[];
    endInstructions: TransactionInstruction[];
    signers: Signer[];
    instructionTypes: string[];
    endInstructionTypes: string[];
    lookupTableAddress: string[];
  } {
    return {
      instructions: this.instructions,
      endInstructions: this.endInstructions,
      signers: this.signers,
      instructionTypes: this.instructionTypes,
      endInstructionTypes: this.endInstructionTypes,
      lookupTableAddress: this.lookupTableAddress,
    };
  }

  /** 返回所有需加入交易的指令（按顺序拼接普通指令与尾部指令） */
  get allInstructions(): TransactionInstruction[] {
    return [...this.instructions, ...this.endInstructions];
  }

  /**
   * 自动从 solanacompass 拉取网络费用，估算 compute budget：
   * - units：默认 600,000
   * - microLamports：按平均费率换算并限幅到 25,000
   */
  public async getComputeBudgetConfig(): Promise<ComputeBudgetConfig | undefined> {
    const json = (
      await axios.get<SolanaFeeInfoJson>(`https://solanacompass.com/api/fees?cacheFreshTime=${5 * 60 * 1000}`)
    ).data;
    const { avg } = json?.[15] ?? {};
    if (!avg) return undefined;
    return {
      units: 600000,
      microLamports: Math.min(Math.ceil((avg * 1000000) / 600000), 25000),
    };
  }

  /** 手动注入 compute budget 指令（存在则插入队首） */
  public addCustomComputeBudget(config?: ComputeBudgetConfig): boolean {
    if (config) {
      const { instructions, instructionTypes } = addComputeBudget(config);
      this.instructions.unshift(...instructions);
      this.instructionTypes.unshift(...instructionTypes);
      return true;
    }
    return false;
  }

  /** 添加小费（tip）转账指令到尾部 */
  public addTipInstruction(tipConfig?: TxTipConfig): boolean {
    if (tipConfig) {
      this.endInstructions.push(
        SystemProgram.transfer({
          fromPubkey: tipConfig.feePayer ?? this.feePayer,
          toPubkey: new PublicKey(tipConfig.address),
          lamports: BigInt(tipConfig.amount.toString()),
        }),
      );
      this.endInstructionTypes.push(InstructionType.TransferTip);
      return true;
    }
    return false;
  }

  /**
   * 计算并注入 compute budget：
   * - 若传入 config 使用之，否则自动获取网络费况估算
   * - 若失败则退化为拼入 defaultIns（如模块自带的默认预算）
   */
  public async calComputeBudget({
    config: propConfig,
    defaultIns,
  }: {
    config?: ComputeBudgetConfig;
    defaultIns?: TransactionInstruction[];
  }): Promise<void> {
    try {
      const config = propConfig || (await this.getComputeBudgetConfig());
      if (this.addCustomComputeBudget(config)) return;
      defaultIns && this.instructions.unshift(...defaultIns);
    } catch {
      defaultIns && this.instructions.unshift(...defaultIns);
    }
  }

  /**
   * 收集一组指令到构建器：普通/尾部指令、签名者、类型标记与 ALT 地址。
   */
  public addInstruction({
    instructions = [],
    endInstructions = [],
    signers = [],
    instructionTypes = [],
    endInstructionTypes = [],
    lookupTableAddress = [],
  }: AddInstructionParam): TxBuilder {
    this.instructions.push(...instructions);
    this.endInstructions.push(...endInstructions);
    this.signers.push(...signers);
    this.instructionTypes.push(...instructionTypes);
    this.endInstructionTypes.push(...endInstructionTypes);
    this.lookupTableAddress.push(...lookupTableAddress.filter((address) => address !== PublicKey.default.toString()));
    return this;
  }

  /** 按指定版本（Legacy/V0）构建单笔交易 */
  public async versionBuild<O = Record<string, any>>({
    txVersion,
    extInfo,
  }: {
    txVersion?: TxVersion;
    extInfo?: O;
  }): Promise<MakeTxData<TxVersion.LEGACY, O> | MakeTxData<TxVersion.V0, O>> {
    if (txVersion === TxVersion.V0) return (await this.buildV0({ ...(extInfo || {}) })) as MakeTxData<TxVersion.V0, O>;
    return this.build<O>(extInfo) as MakeTxData<TxVersion.LEGACY, O>;
  }

  /** 构建 Legacy 交易并返回可执行对象（execute） */
  public build<O = Record<string, any>>(extInfo?: O): MakeTxData<TxVersion.LEGACY, O> {
    const transaction = new Transaction();
    if (this.allInstructions.length) transaction.add(...this.allInstructions);
    transaction.feePayer = this.feePayer;
    if (this.owner?.signer && !this.signers.some((s) => s.publicKey.equals(this.owner!.publicKey)))
      this.signers.push(this.owner.signer);

    return {
      builder: this,
      transaction,
      signers: this.signers,
      instructionTypes: [...this.instructionTypes, ...this.endInstructionTypes],
      execute: async (params) => {
        const { recentBlockHash: propBlockHash, skipPreflight = true, sendAndConfirm, notSendToRpc } = params || {};
        const recentBlockHash = propBlockHash ?? (await getRecentBlockHash(this.connection, this.blockhashCommitment));
        transaction.recentBlockhash = recentBlockHash;
        if (this.signers.length) transaction.sign(...this.signers);

        printSimulate([transaction]);
        if (this.owner?.isKeyPair) {
          const txId = sendAndConfirm
            ? await sendAndConfirmTransaction(
                this.connection,
                transaction,
                this.signers.find((s) => s.publicKey.equals(this.owner!.publicKey))
                  ? this.signers
                  : [...this.signers, this.owner.signer!],
                { skipPreflight },
              )
            : await this.connection.sendRawTransaction(transaction.serialize(), { skipPreflight });

          return {
            txId,
            signedTx: transaction,
          };
        }
        if (this.signAllTransactions) {
          const txs = await this.signAllTransactions([transaction]);
          if (this.signers.length) {
            for (const item of txs) {
              try {
                item.sign(...this.signers);
              } catch (e) {
                //
              }
            }
          }
          return {
            txId: notSendToRpc ? "" : await this.connection.sendRawTransaction(txs[0].serialize(), { skipPreflight }),
            signedTx: txs[0],
          };
        }
        throw new Error("please provide owner in keypair format or signAllTransactions function");
      },
      extInfo: extInfo || ({} as O),
    };
  }

  /** 构建多笔 Legacy 交易（可串行/并行执行），支持拼入额外预构建交易 */
  public buildMultiTx<T = Record<string, any>>(params: {
    extraPreBuildData?: MakeTxData<TxVersion.LEGACY>[];
    extInfo?: T;
  }): MultiTxBuildData {
    const { extraPreBuildData = [], extInfo } = params;
    const { transaction } = this.build(extInfo);

    const filterExtraBuildData = extraPreBuildData.filter((data) => data.transaction.instructions.length > 0);

    const allTransactions: Transaction[] = [transaction, ...filterExtraBuildData.map((data) => data.transaction)];
    const allSigners: Signer[][] = [this.signers, ...filterExtraBuildData.map((data) => data.signers)];
    const allInstructionTypes: string[] = [
      ...this.instructionTypes,
      ...filterExtraBuildData.map((data) => data.instructionTypes).flat(),
    ];

    if (this.owner?.signer) {
      allSigners.forEach((signers) => {
        if (!signers.some((s) => s.publicKey.equals(this.owner!.publicKey))) this.signers.push(this.owner!.signer!);
      });
    }

    return {
      builder: this,
      transactions: allTransactions,
      signers: allSigners,
      instructionTypes: allInstructionTypes,
      execute: async (executeParams?: MultiTxExecuteParam) => {
        const {
          sequentially,
          onTxUpdate,
          skipTxCount = 0,
          recentBlockHash: propBlockHash,
          skipPreflight = true,
        } = executeParams || {};
        const recentBlockHash = propBlockHash ?? (await getRecentBlockHash(this.connection, this.blockhashCommitment));
        if (this.owner?.isKeyPair) {
          if (sequentially) {
            const txIds: string[] = [];
            let i = 0;
            for (const tx of allTransactions) {
              ++i;
              if (i <= skipTxCount) continue;
              const txId = await sendAndConfirmTransaction(
                this.connection,
                tx,
                this.signers.find((s) => s.publicKey.equals(this.owner!.publicKey))
                  ? this.signers
                  : [...this.signers, this.owner.signer!],
                { skipPreflight },
              );
              txIds.push(txId);
            }

            return {
              txIds,
              signedTxs: allTransactions,
            };
          }
          return {
            txIds: await await Promise.all(
              allTransactions.map(async (tx) => {
                tx.recentBlockhash = recentBlockHash;
                return await this.connection.sendRawTransaction(tx.serialize(), { skipPreflight });
              }),
            ),
            signedTxs: allTransactions,
          };
        }

        if (this.signAllTransactions) {
          const partialSignedTxs = allTransactions.map((tx, idx): Transaction => {
            tx.recentBlockhash = recentBlockHash;
            if (allSigners[idx].length) tx.sign(...allSigners[idx]);
            return tx;
          });
          printSimulate(partialSignedTxs);
          const signedTxs = await this.signAllTransactions(partialSignedTxs);
          if (sequentially) {
            let i = 0;
            const processedTxs: TxUpdateParams[] = [];
            const checkSendTx = async (): Promise<void> => {
              if (!signedTxs[i]) return;
              const txId = await this.connection.sendRawTransaction(signedTxs[i].serialize(), { skipPreflight });
              processedTxs.push({ txId, status: "sent", signedTx: signedTxs[i] });
              onTxUpdate?.([...processedTxs]);
              i++;
              let confirmed = false;
              // eslint-disable-next-line
              let intervalId: NodeJS.Timer | null = null,
                subSignatureId: number | null = null;
              const cbk = (signatureResult: SignatureResult): void => {
                intervalId !== null && clearInterval(intervalId);
                subSignatureId !== null && this.connection.removeSignatureListener(subSignatureId);
                const targetTxIdx = processedTxs.findIndex((tx) => tx.txId === txId);
                if (targetTxIdx > -1) {
                  if (processedTxs[targetTxIdx].status === "error" || processedTxs[targetTxIdx].status === "success")
                    return;
                  processedTxs[targetTxIdx].status = signatureResult.err ? "error" : "success";
                }
                onTxUpdate?.([...processedTxs]);
                if (!signatureResult.err) checkSendTx();
              };

              if (this.loopMultiTxStatus)
                intervalId = setInterval(async () => {
                  if (confirmed) {
                    clearInterval(intervalId!);
                    return;
                  }
                  try {
                    const r = await this.connection.getTransaction(txId, {
                      commitment: "confirmed",
                      maxSupportedTransactionVersion: TxVersion.V0,
                    });
                    if (r) {
                      confirmed = true;
                      clearInterval(intervalId!);
                      cbk({ err: r.meta?.err || null });
                      console.log("tx status from getTransaction:", txId);
                    }
                  } catch (e) {
                    confirmed = true;
                    clearInterval(intervalId!);
                    console.error("getTransaction timeout:", e, txId);
                  }
                }, LOOP_INTERVAL);

              subSignatureId = this.connection.onSignature(
                txId,
                (result) => {
                  if (confirmed) {
                    this.connection.removeSignatureListener(subSignatureId!);
                    return;
                  }
                  confirmed = true;
                  cbk(result);
                },
                "confirmed",
              );
              this.connection.getSignatureStatus(txId);
            };
            await checkSendTx();
            return {
              txIds: processedTxs.map((d) => d.txId),
              signedTxs,
            };
          } else {
            const txIds: string[] = [];
            for (let i = 0; i < signedTxs.length; i += 1) {
              const txId = await this.connection.sendRawTransaction(signedTxs[i].serialize(), { skipPreflight });
              txIds.push(txId);
            }
            return {
              txIds,
              signedTxs,
            };
          }
        }
        throw new Error("please provide owner in keypair format or signAllTransactions function");
      },
      extInfo: extInfo || {},
    };
  }

  /** 按指定版本构建多笔交易（Legacy/V0） */
  public async versionMultiBuild<T extends TxVersion, O = Record<string, any>>({
    extraPreBuildData,
    txVersion,
    extInfo,
  }: {
    extraPreBuildData?: MakeTxData<TxVersion.V0>[] | MakeTxData<TxVersion.LEGACY>[];
    txVersion?: T;
    extInfo?: O;
  }): Promise<MakeMultiTxData<T, O>> {
    if (txVersion === TxVersion.V0)
      return (await this.buildV0MultiTx({
        extraPreBuildData: extraPreBuildData as MakeTxData<TxVersion.V0>[],
        buildProps: extInfo || {},
      })) as MakeMultiTxData<T, O>;
    return this.buildMultiTx<O>({
      extraPreBuildData: extraPreBuildData as MakeTxData<TxVersion.LEGACY>[],
      extInfo,
    }) as MakeMultiTxData<T, O>;
  }

  /** 构建单笔 V0 交易：自动拼装 ALT、支持 forerunCreate 与外部 recentBlockhash */
  public async buildV0<O = Record<string, any>>(
    props?: O & {
      lookupTableCache?: CacheLTA;
      lookupTableAddress?: string[];
      forerunCreate?: boolean;
      recentBlockhash?: string;
    },
  ): Promise<MakeTxData<TxVersion.V0, O>> {
    const {
      lookupTableCache = {},
      lookupTableAddress = [],
      forerunCreate,
      recentBlockhash: propRecentBlockhash,
      ...extInfo
    } = props || {};

    const lookupTableAddressAccount = {
      ...(this.cluster === "devnet" ? await getDevLookupTableCache(this.connection) : LOOKUP_TABLE_CACHE),
      ...lookupTableCache,
    };
    const allLTA = Array.from(new Set<string>([...lookupTableAddress, ...this.lookupTableAddress]));
    const needCacheLTA: PublicKey[] = [];
    for (const item of allLTA) {
      if (lookupTableAddressAccount[item] === undefined) needCacheLTA.push(new PublicKey(item));
    }
    const newCacheLTA = await getMultipleLookupTableInfo({ connection: this.connection, address: needCacheLTA });
    for (const [key, value] of Object.entries(newCacheLTA)) lookupTableAddressAccount[key] = value;

    const recentBlockhash = forerunCreate
      ? PublicKey.default.toBase58()
      : propRecentBlockhash ?? (await getRecentBlockHash(this.connection, this.blockhashCommitment));
    const messageV0 = new TransactionMessage({
      payerKey: this.feePayer,
      recentBlockhash,
      instructions: [...this.allInstructions],
    }).compileToV0Message(Object.values(lookupTableAddressAccount));
    if (this.owner?.signer && !this.signers.some((s) => s.publicKey.equals(this.owner!.publicKey)))
      this.signers.push(this.owner.signer);
    const transaction = new VersionedTransaction(messageV0);

    transaction.sign(this.signers);

    return {
      builder: this,
      transaction,
      signers: this.signers,
      instructionTypes: [...this.instructionTypes, ...this.endInstructionTypes],
      execute: async (params) => {
        const { skipPreflight = true, sendAndConfirm, notSendToRpc } = params || {};
        printSimulate([transaction]);
        if (this.owner?.isKeyPair) {
          const txId = await this.connection.sendTransaction(transaction, { skipPreflight });
          if (sendAndConfirm) {
            await confirmTransaction(this.connection, txId);
          }

          return {
            txId,
            signedTx: transaction,
          };
        }
        if (this.signAllTransactions) {
          const txs = await this.signAllTransactions<VersionedTransaction>([transaction]);
          if (this.signers.length) {
            for (const item of txs) {
              try {
                item.sign(this.signers);
              } catch (e) {
                //
              }
            }
          }
          return {
            txId: notSendToRpc ? "" : await this.connection.sendTransaction(txs[0], { skipPreflight }),
            signedTx: txs[0],
          };
        }
        throw new Error("please provide owner in keypair format or signAllTransactions function");
      },
      extInfo: (extInfo || {}) as O,
    };
  }

  /** 构建多笔 V0 交易：整合 ALT、签名与执行（支持串行） */
  public async buildV0MultiTx<T = Record<string, any>>(params: {
    extraPreBuildData?: MakeTxData<TxVersion.V0>[];
    buildProps?: T & {
      lookupTableCache?: CacheLTA;
      lookupTableAddress?: string[];
      forerunCreate?: boolean;
      recentBlockhash?: string;
    };
  }): Promise<MultiTxV0BuildData> {
    const { extraPreBuildData = [], buildProps } = params;
    const { transaction } = await this.buildV0(buildProps);

    const filterExtraBuildData = extraPreBuildData.filter((data) => data.builder.instructions.length > 0);

    const allTransactions: VersionedTransaction[] = [
      transaction,
      ...filterExtraBuildData.map((data) => data.transaction),
    ];
    const allSigners: Signer[][] = [this.signers, ...filterExtraBuildData.map((data) => data.signers)];
    const allInstructionTypes: string[] = [
      ...this.instructionTypes,
      ...filterExtraBuildData.map((data) => data.instructionTypes).flat(),
    ];

    if (this.owner?.signer) {
      allSigners.forEach((signers) => {
        if (!signers.some((s) => s.publicKey.equals(this.owner!.publicKey))) this.signers.push(this.owner!.signer!);
      });
    }

    allTransactions.forEach(async (tx, idx) => {
      tx.sign(allSigners[idx]);
    });

    return {
      builder: this,
      transactions: allTransactions,
      signers: allSigners,
      instructionTypes: allInstructionTypes,
      buildProps,
      execute: async (executeParams?: MultiTxExecuteParam) => {
        const { sequentially, onTxUpdate, recentBlockHash: propBlockHash, skipPreflight = true } = executeParams || {};
        if (propBlockHash) allTransactions.forEach((tx) => (tx.message.recentBlockhash = propBlockHash));
        printSimulate(allTransactions);
        if (this.owner?.isKeyPair) {
          if (sequentially) {
            const txIds: string[] = [];
            for (const tx of allTransactions) {
              const txId = await this.connection.sendTransaction(tx, { skipPreflight });
              await confirmTransaction(this.connection, txId);
              txIds.push(txId);
            }

            return { txIds, signedTxs: allTransactions };
          }

          return {
            txIds: await Promise.all(
              allTransactions.map(async (tx) => {
                return await this.connection.sendTransaction(tx, { skipPreflight });
              }),
            ),
            signedTxs: allTransactions,
          };
        }

        if (this.signAllTransactions) {
          const signedTxs = await this.signAllTransactions(allTransactions);

          if (sequentially) {
            let i = 0;
            const processedTxs: TxUpdateParams[] = [];
            const checkSendTx = async (): Promise<void> => {
              if (!signedTxs[i]) return;
              const txId = await this.connection.sendTransaction(signedTxs[i], { skipPreflight });
              processedTxs.push({ txId, status: "sent", signedTx: signedTxs[i] });
              onTxUpdate?.([...processedTxs]);
              i++;

              let confirmed = false;
              // eslint-disable-next-line
              let intervalId: NodeJS.Timer | null = null,
                subSignatureId: number | null = null;
              const cbk = (signatureResult: SignatureResult): void => {
                intervalId !== null && clearInterval(intervalId);
                subSignatureId !== null && this.connection.removeSignatureListener(subSignatureId);
                const targetTxIdx = processedTxs.findIndex((tx) => tx.txId === txId);
                if (targetTxIdx > -1) {
                  if (processedTxs[targetTxIdx].status === "error" || processedTxs[targetTxIdx].status === "success")
                    return;
                  processedTxs[targetTxIdx].status = signatureResult.err ? "error" : "success";
                }
                onTxUpdate?.([...processedTxs]);
                if (!signatureResult.err) checkSendTx();
              };

              if (this.loopMultiTxStatus)
                intervalId = setInterval(async () => {
                  if (confirmed) {
                    clearInterval(intervalId!);
                    return;
                  }
                  try {
                    const r = await this.connection.getTransaction(txId, {
                      commitment: "confirmed",
                      maxSupportedTransactionVersion: TxVersion.V0,
                    });
                    if (r) {
                      confirmed = true;
                      clearInterval(intervalId!);
                      cbk({ err: r.meta?.err || null });
                      console.log("tx status from getTransaction:", txId);
                    }
                  } catch (e) {
                    confirmed = true;
                    clearInterval(intervalId!);
                    console.error("getTransaction timeout:", e, txId);
                  }
                }, LOOP_INTERVAL);

              subSignatureId = this.connection.onSignature(
                txId,
                (result) => {
                  if (confirmed) {
                    this.connection.removeSignatureListener(subSignatureId!);
                    return;
                  }
                  confirmed = true;
                  cbk(result);
                },
                "confirmed",
              );
              this.connection.getSignatureStatus(txId);
            };
            checkSendTx();
            return {
              txIds: [],
              signedTxs,
            };
          } else {
            const txIds: string[] = [];
            for (let i = 0; i < signedTxs.length; i += 1) {
              const txId = await this.connection.sendTransaction(signedTxs[i], { skipPreflight });
              txIds.push(txId);
            }
            return { txIds, signedTxs };
          }
        }
        throw new Error("please provide owner in keypair format or signAllTransactions function");
      },
      extInfo: buildProps || {},
    };
  }

  /**
   * 将指令按大小限制切分为多笔 Legacy 交易：
   * - computeBudgetConfig：可选统一的预算指令
   * - splitIns：建议拆分点（遇到该指令则强制换新交易）
   */
  public async sizeCheckBuild(
    props?: Record<string, any> & { computeBudgetConfig?: ComputeBudgetConfig; splitIns?: TransactionInstruction[] },
  ): Promise<MultiTxBuildData> {
    const { splitIns = [], computeBudgetConfig, ...extInfo } = props || {};
    const computeBudgetData: { instructions: TransactionInstruction[]; instructionTypes: string[] } =
      computeBudgetConfig
        ? addComputeBudget(computeBudgetConfig)
        : {
            instructions: [],
            instructionTypes: [],
          };

    const signerKey: { [key: string]: Signer } = this.signers.reduce(
      (acc, cur) => ({ ...acc, [cur.publicKey.toBase58()]: cur }),
      {},
    );

    const allTransactions: Transaction[] = [];
    const allSigners: Signer[][] = [];

    let instructionQueue: TransactionInstruction[] = [];
    let splitInsIdx = 0;
    this.allInstructions.forEach((item) => {
      const _itemIns = [...instructionQueue, item];
      const _itemInsWithCompute = computeBudgetConfig ? [...computeBudgetData.instructions, ..._itemIns] : _itemIns;
      const _signerStrs = new Set<string>(
        _itemIns.map((i) => i.keys.filter((ii) => ii.isSigner).map((ii) => ii.pubkey.toString())).flat(),
      );
      const _signer = [..._signerStrs.values()].map((i) => new PublicKey(i));

      if (
        item !== splitIns[splitInsIdx] &&
        instructionQueue.length < 12 &&
        (checkLegacyTxSize({ instructions: _itemInsWithCompute, payer: this.feePayer, signers: _signer }) ||
          checkLegacyTxSize({ instructions: _itemIns, payer: this.feePayer, signers: _signer }))
      ) {
        // current ins add to queue still not exceed tx size limit
        instructionQueue.push(item);
      } else {
        if (instructionQueue.length === 0) throw Error("item ins too big");
        splitInsIdx += item === splitIns[splitInsIdx] ? 1 : 0;
        // if add computeBudget still not exceed tx size limit
        if (
          checkLegacyTxSize({
            instructions: computeBudgetConfig
              ? [...computeBudgetData.instructions, ...instructionQueue]
              : [...instructionQueue],
            payer: this.feePayer,
            signers: _signer,
          })
        ) {
          allTransactions.push(new Transaction().add(...computeBudgetData.instructions, ...instructionQueue));
        } else {
          allTransactions.push(new Transaction().add(...instructionQueue));
        }
        allSigners.push(
          Array.from(
            new Set<string>(
              instructionQueue.map((i) => i.keys.filter((ii) => ii.isSigner).map((ii) => ii.pubkey.toString())).flat(),
            ),
          )
            .map((i) => signerKey[i])
            .filter((i) => i !== undefined),
        );
        instructionQueue = [item];
      }
    });

    if (instructionQueue.length > 0) {
      const _signerStrs = new Set<string>(
        instructionQueue.map((i) => i.keys.filter((ii) => ii.isSigner).map((ii) => ii.pubkey.toString())).flat(),
      );
      const _signers = [..._signerStrs.values()].map((i) => signerKey[i]).filter((i) => i !== undefined);

      if (
        checkLegacyTxSize({
          instructions: computeBudgetConfig
            ? [...computeBudgetData.instructions, ...instructionQueue]
            : [...instructionQueue],
          payer: this.feePayer,
          signers: _signers.map((s) => s.publicKey),
        })
      ) {
        allTransactions.push(new Transaction().add(...computeBudgetData.instructions, ...instructionQueue));
      } else {
        allTransactions.push(new Transaction().add(...instructionQueue));
      }
      allSigners.push(_signers);
    }
    allTransactions.forEach((tx) => (tx.feePayer = this.feePayer));

    if (this.owner?.signer) {
      allSigners.forEach((signers) => {
        if (!signers.some((s) => s.publicKey.equals(this.owner!.publicKey))) signers.push(this.owner!.signer!);
      });
    }

    return {
      builder: this,
      transactions: allTransactions,
      signers: allSigners,
      instructionTypes: this.instructionTypes,
      execute: async (executeParams?: MultiTxExecuteParam) => {
        const {
          sequentially,
          onTxUpdate,
          skipTxCount = 0,
          recentBlockHash: propBlockHash,
          skipPreflight = true,
        } = executeParams || {};
        const recentBlockHash = propBlockHash ?? (await getRecentBlockHash(this.connection, this.blockhashCommitment));
        allTransactions.forEach(async (tx, idx) => {
          tx.recentBlockhash = recentBlockHash;
          if (allSigners[idx].length) tx.sign(...allSigners[idx]);
        });
        printSimulate(allTransactions);
        if (this.owner?.isKeyPair) {
          if (sequentially) {
            let i = 0;
            const txIds: string[] = [];
            for (const tx of allTransactions) {
              ++i;
              if (i <= skipTxCount) {
                txIds.push("tx skipped");
                continue;
              }
              const txId = await sendAndConfirmTransaction(
                this.connection,
                tx,
                this.signers.find((s) => s.publicKey.equals(this.owner!.publicKey))
                  ? this.signers
                  : [...this.signers, this.owner.signer!],
                { skipPreflight },
              );
              txIds.push(txId);
            }

            return {
              txIds,
              signedTxs: allTransactions,
            };
          }
          return {
            txIds: await Promise.all(
              allTransactions.map(async (tx) => {
                return await this.connection.sendRawTransaction(tx.serialize(), { skipPreflight });
              }),
            ),
            signedTxs: allTransactions,
          };
        }
        if (this.signAllTransactions) {
          const needSignedTx = await this.signAllTransactions(
            allTransactions.slice(skipTxCount, allTransactions.length),
          );
          const signedTxs = [...allTransactions.slice(0, skipTxCount), ...needSignedTx];
          if (sequentially) {
            let i = 0;
            const processedTxs: TxUpdateParams[] = [];
            const checkSendTx = async (): Promise<void> => {
              if (!signedTxs[i]) return;
              if (i < skipTxCount) {
                // success before, do not send again
                processedTxs.push({ txId: "", status: "success", signedTx: signedTxs[i] });
                onTxUpdate?.([...processedTxs]);
                i++;
                checkSendTx();
              }
              const txId = await this.connection.sendRawTransaction(signedTxs[i].serialize(), { skipPreflight });
              processedTxs.push({ txId, status: "sent", signedTx: signedTxs[i] });
              onTxUpdate?.([...processedTxs]);
              i++;

              let confirmed = false;
              // eslint-disable-next-line
              let intervalId: NodeJS.Timer | null = null,
                subSignatureId: number | null = null;
              const cbk = (signatureResult: SignatureResult): void => {
                intervalId !== null && clearInterval(intervalId);
                subSignatureId !== null && this.connection.removeSignatureListener(subSignatureId);
                const targetTxIdx = processedTxs.findIndex((tx) => tx.txId === txId);
                if (targetTxIdx > -1) {
                  if (processedTxs[targetTxIdx].status === "error" || processedTxs[targetTxIdx].status === "success")
                    return;
                  processedTxs[targetTxIdx].status = signatureResult.err ? "error" : "success";
                }
                onTxUpdate?.([...processedTxs]);
                if (!signatureResult.err) checkSendTx();
              };

              if (this.loopMultiTxStatus)
                intervalId = setInterval(async () => {
                  if (confirmed) {
                    clearInterval(intervalId!);
                    return;
                  }
                  try {
                    const r = await this.connection.getTransaction(txId, {
                      commitment: "confirmed",
                      maxSupportedTransactionVersion: TxVersion.V0,
                    });
                    if (r) {
                      confirmed = true;
                      clearInterval(intervalId!);
                      cbk({ err: r.meta?.err || null });
                      console.log("tx status from getTransaction:", txId);
                    }
                  } catch (e) {
                    confirmed = true;
                    clearInterval(intervalId!);
                    console.error("getTransaction timeout:", e, txId);
                  }
                }, LOOP_INTERVAL);

              subSignatureId = this.connection.onSignature(
                txId,
                (result) => {
                  if (confirmed) {
                    this.connection.removeSignatureListener(subSignatureId!);
                    return;
                  }
                  confirmed = true;
                  cbk(result);
                },
                "confirmed",
              );
              this.connection.getSignatureStatus(txId);
            };
            await checkSendTx();
            return {
              txIds: processedTxs.map((d) => d.txId),
              signedTxs,
            };
          } else {
            const txIds: string[] = [];
            for (let i = 0; i < signedTxs.length; i += 1) {
              const txId = await this.connection.sendRawTransaction(signedTxs[i].serialize(), { skipPreflight });
              txIds.push(txId);
            }
            return { txIds, signedTxs };
          }
        }
        throw new Error("please provide owner in keypair format or signAllTransactions function");
      },
      extInfo: extInfo || {},
    };
  }

  /**
   * 将指令按大小限制切分为多笔 V0 交易：
   * - computeBudgetConfig：可选统一的预算指令
   * - lookupTableCache / lookupTableAddress：ALT 缓存与地址
   * - splitIns：建议拆分点
   */
  public async sizeCheckBuildV0(
    props?: Record<string, any> & {
      computeBudgetConfig?: ComputeBudgetConfig;
      lookupTableCache?: CacheLTA;
      lookupTableAddress?: string[];
      splitIns?: TransactionInstruction[];
    },
  ): Promise<MultiTxV0BuildData> {
    const {
      computeBudgetConfig,
      splitIns = [],
      lookupTableCache = {},
      lookupTableAddress = [],
      ...extInfo
    } = props || {};
    const lookupTableAddressAccount = {
      ...(this.cluster === "devnet" ? await getDevLookupTableCache(this.connection) : LOOKUP_TABLE_CACHE),
      ...lookupTableCache,
    };
    const allLTA = Array.from(new Set<string>([...this.lookupTableAddress, ...lookupTableAddress]));
    const needCacheLTA: PublicKey[] = [];
    for (const item of allLTA) {
      if (lookupTableAddressAccount[item] === undefined) needCacheLTA.push(new PublicKey(item));
    }
    const newCacheLTA = await getMultipleLookupTableInfo({ connection: this.connection, address: needCacheLTA });
    for (const [key, value] of Object.entries(newCacheLTA)) lookupTableAddressAccount[key] = value;

    const computeBudgetData: { instructions: TransactionInstruction[]; instructionTypes: string[] } =
      computeBudgetConfig
        ? addComputeBudget(computeBudgetConfig)
        : {
            instructions: [],
            instructionTypes: [],
          };

    const blockHash = await getRecentBlockHash(this.connection, this.blockhashCommitment);

    const signerKey: { [key: string]: Signer } = this.signers.reduce(
      (acc, cur) => ({ ...acc, [cur.publicKey.toBase58()]: cur }),
      {},
    );
    const allTransactions: VersionedTransaction[] = [];
    const allSigners: Signer[][] = [];

    let instructionQueue: TransactionInstruction[] = [];
    let splitInsIdx = 0;
    this.allInstructions.forEach((item) => {
      const _itemIns = [...instructionQueue, item];
      const _itemInsWithCompute = computeBudgetConfig ? [...computeBudgetData.instructions, ..._itemIns] : _itemIns;
      if (
        item !== splitIns[splitInsIdx] &&
        instructionQueue.length < 12 &&
        (checkV0TxSize({ instructions: _itemInsWithCompute, payer: this.feePayer, lookupTableAddressAccount }) ||
          checkV0TxSize({ instructions: _itemIns, payer: this.feePayer, lookupTableAddressAccount }))
      ) {
        // current ins add to queue still not exceed tx size limit
        instructionQueue.push(item);
      } else {
        if (instructionQueue.length === 0) throw Error("item ins too big");
        splitInsIdx += item === splitIns[splitInsIdx] ? 1 : 0;
        const lookupTableAddress: undefined | CacheLTA = {};
        for (const item of [...new Set<string>(allLTA)]) {
          if (lookupTableAddressAccount[item] !== undefined) lookupTableAddress[item] = lookupTableAddressAccount[item];
        }
        // if add computeBudget still not exceed tx size limit
        if (
          computeBudgetConfig &&
          checkV0TxSize({
            instructions: [...computeBudgetData.instructions, ...instructionQueue],
            payer: this.feePayer,
            lookupTableAddressAccount,
            recentBlockhash: blockHash,
          })
        ) {
          const messageV0 = new TransactionMessage({
            payerKey: this.feePayer,
            recentBlockhash: blockHash,

            instructions: [...computeBudgetData.instructions, ...instructionQueue],
          }).compileToV0Message(Object.values(lookupTableAddressAccount));
          allTransactions.push(new VersionedTransaction(messageV0));
        } else {
          const messageV0 = new TransactionMessage({
            payerKey: this.feePayer,
            recentBlockhash: blockHash,
            instructions: [...instructionQueue],
          }).compileToV0Message(Object.values(lookupTableAddressAccount));
          allTransactions.push(new VersionedTransaction(messageV0));
        }
        allSigners.push(
          Array.from(
            new Set<string>(
              instructionQueue.map((i) => i.keys.filter((ii) => ii.isSigner).map((ii) => ii.pubkey.toString())).flat(),
            ),
          )
            .map((i) => signerKey[i])
            .filter((i) => i !== undefined),
        );
        instructionQueue = [item];
      }
    });

    if (instructionQueue.length > 0) {
      const _signerStrs = new Set<string>(
        instructionQueue.map((i) => i.keys.filter((ii) => ii.isSigner).map((ii) => ii.pubkey.toString())).flat(),
      );
      const _signers = [..._signerStrs.values()].map((i) => signerKey[i]).filter((i) => i !== undefined);

      if (
        computeBudgetConfig &&
        checkV0TxSize({
          instructions: [...computeBudgetData.instructions, ...instructionQueue],
          payer: this.feePayer,
          lookupTableAddressAccount,
          recentBlockhash: blockHash,
        })
      ) {
        const messageV0 = new TransactionMessage({
          payerKey: this.feePayer,
          recentBlockhash: blockHash,
          instructions: [...computeBudgetData.instructions, ...instructionQueue],
        }).compileToV0Message(Object.values(lookupTableAddressAccount));
        allTransactions.push(new VersionedTransaction(messageV0));
      } else {
        const messageV0 = new TransactionMessage({
          payerKey: this.feePayer,
          recentBlockhash: blockHash,
          instructions: [...instructionQueue],
        }).compileToV0Message(Object.values(lookupTableAddressAccount));
        allTransactions.push(new VersionedTransaction(messageV0));
      }

      allSigners.push(_signers);
    }

    if (this.owner?.signer) {
      allSigners.forEach((signers) => {
        if (!signers.some((s) => s.publicKey.equals(this.owner!.publicKey))) signers.push(this.owner!.signer!);
      });
    }

    allTransactions.forEach((tx, idx) => {
      tx.sign(allSigners[idx]);
    });

    return {
      builder: this,
      transactions: allTransactions,
      buildProps: props,
      signers: allSigners,
      instructionTypes: this.instructionTypes,
      execute: async (executeParams?: MultiTxExecuteParam) => {
        const {
          sequentially,
          onTxUpdate,
          skipTxCount = 0,
          recentBlockHash: propBlockHash,
          skipPreflight = true,
        } = executeParams || {};
        allTransactions.map(async (tx, idx) => {
          if (allSigners[idx].length) tx.sign(allSigners[idx]);
          if (propBlockHash) tx.message.recentBlockhash = propBlockHash;
        });
        printSimulate(allTransactions);
        if (this.owner?.isKeyPair) {
          if (sequentially) {
            let i = 0;
            const txIds: string[] = [];
            for (const tx of allTransactions) {
              ++i;
              if (i <= skipTxCount) {
                console.log("skip tx: ", i);
                txIds.push("tx skipped");
                continue;
              }
              const txId = await this.connection.sendTransaction(tx, { skipPreflight });
              await confirmTransaction(this.connection, txId);

              txIds.push(txId);
            }

            return { txIds, signedTxs: allTransactions };
          }

          return {
            txIds: await Promise.all(
              allTransactions.map(async (tx) => {
                return await this.connection.sendTransaction(tx, { skipPreflight });
              }),
            ),
            signedTxs: allTransactions,
          };
        }
        if (this.signAllTransactions) {
          const needSignedTx = await this.signAllTransactions(
            allTransactions.slice(skipTxCount, allTransactions.length),
          );
          const signedTxs = [...allTransactions.slice(0, skipTxCount), ...needSignedTx];
          if (sequentially) {
            let i = 0;
            const processedTxs: TxUpdateParams[] = [];
            const checkSendTx = async (): Promise<void> => {
              if (!signedTxs[i]) return;
              if (i < skipTxCount) {
                // success before, do not send again
                processedTxs.push({ txId: "", status: "success", signedTx: signedTxs[i] });
                onTxUpdate?.([...processedTxs]);
                i++;
                checkSendTx();
                return;
              }
              const txId = await this.connection.sendTransaction(signedTxs[i], { skipPreflight });
              processedTxs.push({ txId, status: "sent", signedTx: signedTxs[i] });
              onTxUpdate?.([...processedTxs]);
              i++;

              let confirmed = false;
              // eslint-disable-next-line
              let intervalId: NodeJS.Timer | null = null,
                subSignatureId: number | null = null;
              const cbk = (signatureResult: SignatureResult): void => {
                intervalId !== null && clearInterval(intervalId);
                subSignatureId !== null && this.connection.removeSignatureListener(subSignatureId);
                const targetTxIdx = processedTxs.findIndex((tx) => tx.txId === txId);
                if (targetTxIdx > -1) {
                  if (processedTxs[targetTxIdx].status === "error" || processedTxs[targetTxIdx].status === "success")
                    return;
                  processedTxs[targetTxIdx].status = signatureResult.err ? "error" : "success";
                }
                onTxUpdate?.([...processedTxs]);
                if (!signatureResult.err) checkSendTx();
              };

              if (this.loopMultiTxStatus)
                intervalId = setInterval(async () => {
                  if (confirmed) {
                    clearInterval(intervalId!);
                    return;
                  }
                  try {
                    const r = await this.connection.getTransaction(txId, {
                      commitment: "confirmed",
                      maxSupportedTransactionVersion: TxVersion.V0,
                    });
                    if (r) {
                      confirmed = true;
                      clearInterval(intervalId!);
                      cbk({ err: r.meta?.err || null });
                      console.log("tx status from getTransaction:", txId);
                    }
                  } catch (e) {
                    confirmed = true;
                    clearInterval(intervalId!);
                    console.error("getTransaction timeout:", e, txId);
                  }
                }, LOOP_INTERVAL);

              subSignatureId = this.connection.onSignature(
                txId,
                (result) => {
                  if (confirmed) {
                    this.connection.removeSignatureListener(subSignatureId!);
                    return;
                  }
                  confirmed = true;
                  cbk(result);
                },
                "confirmed",
              );
              this.connection.getSignatureStatus(txId);
            };
            checkSendTx();
            return {
              txIds: [],
              signedTxs,
            };
          } else {
            const txIds: string[] = [];
            for (let i = 0; i < signedTxs.length; i += 1) {
              const txId = await this.connection.sendTransaction(signedTxs[i], { skipPreflight });
              txIds.push(txId);
            }
            return { txIds, signedTxs };
          }
        }
        throw new Error("please provide owner in keypair format or signAllTransactions function");
      },
      extInfo: extInfo || {},
    };
  }
}
