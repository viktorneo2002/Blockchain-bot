import { Connection, PublicKey } from '@solana/web3.js';
import { BN } from '@project-serum/anchor';
import { EventEmitter } from 'events';
export interface LiquidityPool {
    address: PublicKey;
    protocol: ProtocolType;
    tokenA: PublicKey;
    tokenB: PublicKey;
    reserveA: BN;
    reserveB: BN;
    fee: number;
    lastUpdate: number;
    liquidity: BN;
    volume24h: BN;
    apy: number;
}
export interface LiquiditySource {
    protocol: ProtocolType;
    programId: PublicKey;
    pools: Map<string, LiquidityPool>;
    feeStructure: FeeStructure;
    priority: number;
    isActive: boolean;
    lastSync: number;
}
export interface FeeStructure {
    baseFee: number;
    protocolFee: number;
    lpFee: number;
    referralFee?: number;
}
export interface RouteStep {
    pool: LiquidityPool;
    inputAmount: BN;
    outputAmount: BN;
    priceImpact: number;
    fee: BN;
}
export interface OptimalRoute {
    steps: RouteStep[];
    inputAmount: BN;
    outputAmount: BN;
    totalFees: BN;
    priceImpact: number;
    executionPrice: number;
}
export declare enum ProtocolType {
    RAYDIUM = "RAYDIUM",
    ORCA = "ORCA",
    SABER = "SABER",
    MERCURIAL = "MERCURIAL",
    SERUM = "SERUM",
    PHOENIX = "PHOENIX",
    LIFINITY = "LIFINITY",
    MARINADE = "MARINADE",
    ALDRIN = "ALDRIN",
    CREMA = "CREMA",
    CROPPER = "CROPPER",
    SENCHA = "SENCHA",
    SAROS = "SAROS",
    STEPN = "STEPN",
    INVARIANT = "INVARIANT",
    METEORA = "METEORA",
    GOOSEFX = "GOOSEFX",
    HELIUM = "HELIUM"
}
export declare class LiquiditySourceManager extends EventEmitter {
    private connection;
    private sources;
    private poolCache;
    private tokenPairIndex;
    private updateInterval;
    private readonly CACHE_TTL;
    private readonly MAX_ROUTE_HOPS;
    private readonly MIN_LIQUIDITY_USD;
    private readonly MAX_PRICE_IMPACT;
    private subscriptions;
    constructor(connection: Connection);
    private initializeSources;
    initialize(): Promise<void>;
    private syncAllPools;
    private syncPoolsForSource;
    private getPoolDataSize;
    private parsePoolData;
    private getPoolBalances;
    private calculatePoolFee;
    private calculateLiquidity;
    private updateTokenPairIndex;
    private getPairKey;
    findOptimalRoute(inputToken: PublicKey, outputToken: PublicKey, inputAmount: BN, maxSlippage?: number): OptimalRoute | null;
    private getDirectPools;
    private calculateDirectRoute;
    private calculateSwapOutput;
    private findMultiHopRoute;
    private getConnectedTokens;
    private calculateSplitRoute;
    subscribeToPool(poolAddress: PublicKey): Promise<void>;
    unsubscribeFromPool(poolAddress: PublicKey): Promise<void>;
    private startPoolUpdates;
    getPoolsByToken(token: PublicKey): LiquidityPool[];
    getTopPools(limit?: number): LiquidityPool[];
    updatePoolMetrics(poolAddress: PublicKey, volume24h: BN, apy: number): Promise<void>;
    getTotalLiquidity(): BN;
    getActiveProtocols(): ProtocolType[];
    shutdown(): Promise<void>;
    getPoolByAddress(address: PublicKey): LiquidityPool | undefined;
    clearCache(): void;
    refreshPool(poolAddress: PublicKey): Promise<LiquidityPool | null>;
    private removePool;
    getHealthStatus(): {
        isHealthy: boolean;
        activeSources: number;
        totalPools: number;
        lastUpdate: number;
        errors: string[];
    };
    setSourceActive(protocol: ProtocolType, active: boolean): void;
    getRouteMetrics(route: OptimalRoute): {
        effectivePrice: number;
        priceImpactBps: number;
        totalFeeBps: number;
        liquidityUsed: BN;
    };
    batchRefreshPools(poolAddresses: PublicKey[]): Promise<Map<string, LiquidityPool | null>>;
    getProtocolStats(protocol: ProtocolType): {
        poolCount: number;
        totalLiquidity: BN;
        totalVolume24h: BN;
        averageAPY: number;
        topPools: LiquidityPool[];
    } | null;
    estimateGasForRoute(route: OptimalRoute): number;
    validateRoute(route: OptimalRoute): {
        isValid: boolean;
        errors: string[];
    };
    warmupCache(tokens: PublicKey[]): Promise<void>;
    getSlippageAdjustedOutput(route: OptimalRoute, slippageBps: number): BN;
    getHistoricalMetrics(poolAddress: PublicKey, duration?: number): Promise<{
        averageLiquidity: BN;
        volumeTotal: BN;
        priceVolatility: number;
    } | null>;
    getArbitrageOpportunities(minProfitBps?: number): Array<{
        tokenA: PublicKey;
        tokenB: PublicKey;
        buyPool: LiquidityPool;
        sellPool: LiquidityPool;
        profitBps: number;
    }>;
    simulateRoute(route: OptimalRoute, slippageBps?: number): Promise<{
        success: boolean;
        finalOutput: BN;
        actualPriceImpact: number;
        gasEstimate: number;
    }>;
    getPriceQuote(inputToken: PublicKey, outputToken: PublicKey, inputAmount: BN): {
        price: number;
        availableLiquidity: BN;
        sources: number;
    } | null;
    monitorPoolHealth(poolAddress: PublicKey): Promise<{
        isHealthy: boolean;
        issues: string[];
        recommendations: string[];
    }>;
    optimizeRouteForGas(route: OptimalRoute): OptimalRoute;
    getPoolUtilization(poolAddress: PublicKey): number;
    prefetchPoolsForTokens(tokens: PublicKey[]): Promise<void>;
    exportMetrics(): {
        protocolStats: Map<ProtocolType, any>;
        poolCount: number;
        totalLiquidity: string;
        activeSubscriptions: number;
        cacheHitRate: number;
        lastUpdateTime: number;
    };
}
export default LiquiditySourceManager;
//# sourceMappingURL=LiquiditySourceManager.d.ts.map