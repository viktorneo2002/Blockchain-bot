import { Connection, PublicKey } from '@solana/web3.js';
import BN from 'bn.js';
import { EventEmitter } from 'events';
export interface StrategyConfig {
    minProfitBps: number;
    maxPositionSize: BN;
    maxSlippageBps: number;
    minLiquidityThreshold: BN;
    maxGasPrice: BN;
    riskLimit: number;
    rebalanceThreshold: number;
    dynamicFeeAdjustment: boolean;
    mevProtection: boolean;
    frontrunDetection: boolean;
    multiPoolArbitrage: boolean;
    volumeWeightedEntry: boolean;
}
export interface MarketData {
    price: number;
    volume24h: BN;
    liquidity: BN;
    volatility: number;
    spread: number;
    depth: {
        bids: BN;
        asks: BN;
    };
    momentum: number;
    vwap: number;
}
export interface Position {
    token: PublicKey;
    amount: BN;
    entryPrice: number;
    timestamp: number;
    unrealizedPnl: number;
    strategy: StrategyType;
}
export interface Signal {
    type: SignalType;
    strength: number;
    token: PublicKey;
    action: 'BUY' | 'SELL' | 'HOLD';
    targetPrice: number;
    stopLoss: number;
    takeProfit: number;
    confidence: number;
    timeframe: number;
}
export declare enum StrategyType {
    ARBITRAGE = "ARBITRAGE",
    MOMENTUM = "MOMENTUM",
    MEAN_REVERSION = "MEAN_REVERSION",
    LIQUIDITY_PROVISION = "LIQUIDITY_PROVISION",
    STATISTICAL_ARB = "STATISTICAL_ARB",
    MARKET_MAKING = "MARKET_MAKING"
}
export declare enum SignalType {
    STRONG_BUY = "STRONG_BUY",
    BUY = "BUY",
    NEUTRAL = "NEUTRAL",
    SELL = "SELL",
    STRONG_SELL = "STRONG_SELL"
}
export declare class StrategyEngine extends EventEmitter {
    private connection;
    private config;
    private positions;
    private marketCache;
    private performanceMetrics;
    private isRunning;
    private strategies;
    private riskManager;
    private signalAggregator;
    constructor(connection: Connection, config: StrategyConfig);
    private initializeStrategies;
    start(): Promise<void>;
    stop(): Promise<void>;
    private runStrategies;
    private fetchMarketData;
    private generateSignals;
    private validateSignals;
    private isSignalValid;
    private executeSignals;
    private executeSignal;
    private buildInstructions;
    private sendOptimizedTransaction;
    private prioritizeSignals;
    private calculateSignalScore;
    private updatePositions;
    private shouldClosePosition;
    private performRiskCheck;
    private calculateTotalExposure;
    private adjustStrategyParameters;
    private detectMEV;
    private detectFrontrun;
    private scanMempool;
    private getPendingTransactions;
    private isSimilarTransaction;
    private decodeTokenInstruction;
    private estimateGasPrice;
    private calculatePriorityFee;
    private isHighPriorityTime;
    private getMonitoredTokens;
    private getTokenMarketData;
    private fetchTokenPrice;
    private fetchTokenVolume;
    private fetchTokenLiquidity;
    private fetchOrderbook;
    private calculateVolatility;
    private calculateMomentum;
    private calculateVWAP;
    private calculateSpread;
    private buildBuyInstructions;
    private buildSellInstructions;
    private updatePosition;
    private closePosition;
    private closeAllPositions;
    private reduceExposure;
    private handleError;
    private sleep;
}
export interface EngineMetrics {
    uptime: number;
    totalPositions: number;
    activeSharpe: number;
    dailyPnL: number;
    successRate: number;
}
export interface ExecutionReport {
    timestamp: number;
    signal: Signal;
    status: 'success' | 'failed' | 'partial';
    fillPrice?: number;
    slippage?: number;
    gasUsed?: BN;
}
export declare class StrategyEngineBuilder {
    private config;
    withMinProfit(bps: number): this;
    withMaxPosition(size: BN): this;
    withSlippage(bps: number): this;
    withLiquidity(threshold: BN): this;
    withGasLimit(maxGas: BN): this;
    withRiskLimit(limit: number): this;
    withRebalance(threshold: number): this;
    enableDynamicFees(): this;
    enableMEVProtection(): this;
    enableFrontrunDetection(): this;
    enableMultiPool(): this;
    enableVolumeWeighting(): this;
    build(connection: Connection): StrategyEngine;
}
export declare const createOptimizedEngine: (connection: Connection) => StrategyEngine;
export default StrategyEngine;
//# sourceMappingURL=StrategyEngine.d.ts.map