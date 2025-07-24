import { PublicKey, Transaction, VersionedTransaction } from '@solana/web3.js';
import { EventEmitter } from 'events';
import { BN } from '@project-serum/anchor';
interface MonitorConfig {
    rpcEndpoint: string;
    wsEndpoint: string;
    commitment: 'processed' | 'confirmed' | 'finalized';
    targetPrograms: string[];
    minProfitThreshold: number;
    maxLatencyMs: number;
    reconnectDelayMs: number;
    maxReconnectAttempts: number;
}
interface PendingTransaction {
    signature: string;
    transaction: Transaction | VersionedTransaction;
    slot: number;
    timestamp: number;
    programId: PublicKey;
    accounts: PublicKey[];
    data: Buffer;
}
interface TradingOpportunity {
    type: 'frontrun' | 'backrun' | 'sandwich';
    targetTx: PendingTransaction;
    expectedProfit: number;
    gasEstimate: number;
    poolAddress: PublicKey;
    tokenA: PublicKey;
    tokenB: PublicKey;
    amountIn: BN;
    amountOut: BN;
    priceImpact: number;
    slippage: number;
    priority: number;
    deadline: number;
}
interface PoolState {
    address: PublicKey;
    tokenA: PublicKey;
    tokenB: PublicKey;
    reserveA: BN;
    reserveB: BN;
    fee: number;
    lastUpdate: number;
}
export declare class PendingTxMonitor extends EventEmitter {
    private connection;
    private ws;
    private config;
    private isRunning;
    private reconnectAttempts;
    private subscriptionId;
    private seenSignatures;
    private poolCache;
    private pendingTxQueue;
    private processingInterval;
    private metricsInterval;
    private lastBlockTime;
    private metrics;
    private readonly DEX_PROGRAMS;
    constructor(config: MonitorConfig);
    start(): Promise<void>;
    stop(): Promise<void>;
    private connectWebSocket;
    private subscribeToPrograms;
    private handleWebSocketMessage;
    private processLogNotification;
    private fetchTransaction;
    private parseTransaction;
    private startProcessingLoop;
    private processQueuedTransactions;
    private analyzeTradingOpportunity;
    private analyzeRaydiumTx;
    private analyzeOrcaWhirlpoolTx;
    private analyzeOrcaV2Tx;
    private analyzeSerumTx;
    private getPoolState;
    private decodePoolData;
    private calculatePriceImpact;
    private estimateProfit;
    private estimateSerumProfit;
    private calculatePriority;
    private decodeAmount;
    private updateSlotInfo;
    private initializePoolCache;
    private handleDisconnection;
    private handleReconnection;
    private startMetricsReporting;
    getMetrics(): {
        uptime: number;
        successRate: number;
        txPerSecond: number;
        cacheHitRate: number;
        txProcessed: number;
        opportunitiesFound: number;
        avgLatency: number;
        missedOpportunities: number;
        errors: number;
    };
    clearCache(): void;
    updateConfig(config: Partial<MonitorConfig>): void;
    getPoolCache(): Map<string, PoolState>;
    getQueueLength(): number;
    isConnected(): boolean;
    testConnection(): Promise<boolean>;
    on(event: 'opportunity', listener: (opportunity: TradingOpportunity) => void): this;
    on(event: 'error', listener: (error: Error) => void): this;
    on(event: 'connected' | 'disconnected' | 'started' | 'stopped', listener: () => void): this;
    on(event: 'metrics', listener: (metrics: any) => void): this;
    on(event: 'slot', listener: (slot: {
        slot: number;
        timestamp: number;
    }) => void): this;
    removeAllListeners(event?: string | symbol): this;
}
export type { MonitorConfig, PendingTransaction, TradingOpportunity, PoolState };
export default PendingTxMonitor;
//# sourceMappingURL=pendingTxMonitor.d.ts.map