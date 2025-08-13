/*
  文件概览：Raydium 模块基类（ModuleBase）

  作用：
  - 作为各业务模块（如 Account、Liquidity、CLMM、TradeV2 等）的通用基类，统一持有全局上下文 `scope`（Raydium 实例）与模块级日志器。
  - 提供创建交易构建器（TxBuilder）的便捷方法 `createTxBuilder`，自动注入连接、费支付者、集群、owner、API、签名器等上下文信息。
  - 提供统一的日志与错误抛出方法，便于模块内部标准化输出。
  - 支持模块禁用校验（`checkDisabled`）。
*/

import { PublicKey } from "@solana/web3.js";

import { createLogger, Logger } from "../common/logger";
import { TxBuilder } from "../common/txTool/txTool";

import { Raydium } from "./";

/**
 * 模块基类初始化参数
 * - scope：Raydium 实例，用于访问连接、owner、API 等全局上下文
 * - moduleName：模块名，用于日志标识
 */
export interface ModuleBaseProps {
  scope: Raydium;
  moduleName: string;
}

/**
 * 将入参拼接为字符串，供日志/错误输出使用。
 */
const joinMsg = (...args: (string | number | Record<string, any>)[]): string =>
  args
    .map((arg) => {
      try {
        return typeof arg === "object" ? JSON.stringify(arg) : arg;
      } catch {
        return arg;
      }
    })
    .join(", ");

/**
 * 模块基类：
 * - 统一维护 `scope` 与模块级 `logger`
 * - 暴露便捷的 `createTxBuilder` 用于组装交易
 * - 提供标准化日志与错误抛出方法
 */
export default class ModuleBase {
  public scope: Raydium;
  private disabled = false;
  protected logger: Logger;

  /** 构造函数：注入全局上下文与模块名，创建模块级日志器 */
  constructor({ scope, moduleName }: ModuleBaseProps) {
    this.scope = scope;
    this.logger = createLogger(moduleName);
  }

  /**
   * 创建交易构建器 TxBuilder：
   * - 自动校验 owner 是否存在
   * - 注入连接、费支付者、集群、owner、区块哈希承诺等级、循环查询开关、API、批量签名器等
   */
  protected createTxBuilder(feePayer?: PublicKey): TxBuilder {
    this.scope.checkOwner();
    return new TxBuilder({
      connection: this.scope.connection,
      feePayer: feePayer || this.scope.ownerPubKey,
      cluster: this.scope.cluster,
      owner: this.scope.owner,
      blockhashCommitment: this.scope.blockhashCommitment,
      loopMultiTxStatus: this.scope.loopMultiTxStatus,
      api: this.scope.api,
      signAllTransactions: this.scope.signAllTransactions,
    });
  }

  /** 输出 Debug 级日志 */
  public logDebug(...args: (string | number | Record<string, any>)[]): void {
    this.logger.debug(joinMsg(args));
  }

  /** 输出 Info 级日志 */
  public logInfo(...args: (string | number | Record<string, any>)[]): void {
    this.logger.info(joinMsg(args));
  }

  /** 格式化消息并抛出 Error，用于快速终止流程并上抛错误 */
  public logAndCreateError(...args: (string | number | Record<string, any>)[]): void {
    const message = joinMsg(args);
    throw new Error(message);
  }

  /** 校验模块是否被禁用或上下文缺失，若不工作则抛错 */
  public checkDisabled(): void {
    if (this.disabled || !this.scope) this.logAndCreateError("module not working");
  }
}
