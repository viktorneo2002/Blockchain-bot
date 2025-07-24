import { Connection, PublicKey, TransactionInstruction, AddressLookupTableAccount } from '@solana/web3.js';
import BN from 'bn.js';
interface FlashLoanProtocol {
    name: string;
    programId: PublicKey;
    enabled: boolean;
    maxLoanAmount: BN;
    feeRate: number;
    priority: number;
    address?: PublicKey;
    healthCheck: () => Promise<boolean>;
}
interface FlashLoanRequest {
    tokenMint: PublicKey;
    amount: BN;
    targetInstructions: TransactionInstruction[];
    payer: PublicKey;
    priorityFee?: number;
    maxRetries?: number;
    preferredProtocol?: string;
}
interface FlashLoanResponse {
    success: boolean;
    txSignature?: string;
    protocol?: string;
    error?: string;
    gasUsed?: number;
    profit?: BN;
}
interface CircuitBreakerStatus {
    failures: number;
    lastReset: number;
}
export declare class MultiProtocolFlashLoanAggregator {
    private connection;
    private readonly MAX_FAILURES;
    private protocols;
    private lookupTableCache;
    private protocolConfig;
    private maxConcurrentRequests;
    private circuitBreaker;
    private readonly CIRCUIT_BREAKER_THRESHOLD;
    private readonly CIRCUIT_BREAKER_TIMEOUT;
    constructor(connection: Connection);
    private initializeProtocols;
    private checkProtocolHealth;
    private recordProtocolFailure;
    executeFlashLoan(request: FlashLoanRequest): Promise<FlashLoanResponse>;
    private getAvailableProtocols;
    private executeProtocolFlashLoan;
    private executeSolendFlashLoan;
    private executePortFlashLoan;
    private executeLarixFlashLoan;
    private executeApricotFlashLoan;
    private executeKaminoFlashLoan;
    private buildComputeBudgetInstructions;
    private buildSolendBorrowInstruction;
    private buildSolendRepayInstruction;
    private buildPortFlashLoanInstruction;
    private buildLarixFlashLoanInstruction;
    private buildApricotFlashLoanInstruction;
    private buildKaminoFlashLoanInstruction;
    getOptimalProtocol(amount: BN): Promise<FlashLoanProtocol | null>;
    simulateFlashLoan(request: FlashLoanRequest): Promise<{
        estimatedGas: number;
        estimatedFees: BN;
        optimalProtocol: string;
    }>;
    getProtocolLiquidity(protocolName: string): Promise<BN>;
    getAggregatedLiquidity(tokenMint: PublicKey): Promise<BN>;
    executeMultiProtocolFlashLoan(requests: FlashLoanRequest[]): Promise<FlashLoanResponse[]>;
    private chunkArray;
    optimizeFlashLoanRoute(amount: BN, targetInstructions: TransactionInstruction[]): Promise<{
        protocol: string;
        estimatedProfit: BN;
        gasEstimate: number;
    }>;
    private calculateArbitrageRevenue;
    private isSwapInstruction;
    private estimateSwapOutput;
    private decodeJupiterSwap;
    private decodeOrcaSwap;
    private decodeRaydiumV2Swap;
    private decodeRaydiumCLMMSwap;
    private decodeSarosSwap;
    private calculateCLMMOutput;
    private getPoolOutputAmount;
    private estimateGasForProtocol;
    validateFlashLoanRequest(request: FlashLoanRequest): Promise<{
        valid: boolean;
        error?: string;
    }>;
    monitorProtocolHealth(): Promise<Map<string, {
        healthy: boolean;
        liquidity: BN;
        successRate: number;
        avgResponseTime: number;
    }>>;
    updateProtocolPriorities(): Promise<void>;
    preloadLookupTables(addresses: string[]): Promise<void>;
    getLookupTableAccounts(): AddressLookupTableAccount[];
    clearLookupTableCache(): void;
    getProtocolStats(): {
        name: string;
        enabled: boolean;
        feeRate: number;
        maxLoan: string;
        priority: number;
        successRate?: number;
    }[];
    setMaxConcurrentRequests(max: number): void;
    warmupProtocols(): Promise<void>;
    private sleep;
    cleanup(): Promise<void>;
    getPerformanceMetrics(): Promise<{
        totalRequests: number;
        successfulRequests: number;
        failedRequests: number;
        averageProfit: string;
        protocolUsage: Map<string, number>;
    }>;
    emergencyShutdown(): Promise<void>;
    getRecommendedProtocol(amount: BN, urgency?: 'low' | 'medium' | 'high'): Promise<FlashLoanProtocol | null>;
    batchValidateRequests(requests: FlashLoanRequest[]): Promise<Map<number, {
        valid: boolean;
        error?: string;
    }>>;
    getTotalAvailableLiquidity(): Promise<{
        [token: string]: BN;
    }>;
    private getTokenMintAddress;
    private fetchProtocolLiquidity;
    private fetchSolendLiquidity;
    private fetchPortFinanceLiquidity;
    private fetchLarixLiquidity;
    private fetchApricotLiquidity;
    private fetchKaminoLiquidity;
    logProtocolPerformance(): Promise<void>;
    getProtocol(name: string): FlashLoanProtocol | undefined;
    updateProtocolConfig(name: string, config: Partial<FlashLoanProtocol>): boolean;
    calculateProtocolFees(amount: BN): Map<string, BN>;
    estimateExecutionTime(protocolName: string): number;
    getProtocolsByPreference(): FlashLoanProtocol[];
    resetCircuitBreaker(protocolName: string): void;
    getCircuitBreakerStatus(): Map<string, CircuitBreakerStatus>;
    isProtocolBroken(protocolName: string): boolean;
    setProtocolEnabled(protocolName: string, enabled: boolean): boolean;
    getRealTimeRankings(): Promise<Array<{
        name: string;
        score: number;
        metrics: {
            successRate: number;
            avgFee: number;
            liquidity: string;
            responseTime: number;
        };
    }>>;
    executeWithRetry(request: FlashLoanRequest, maxRetries?: number): Promise<FlashLoanResponse>;
    estimateArbitrageAPY(dailyOpportunities: number, avgProfit: BN, capital: BN): number;
}
export default MultiProtocolFlashLoanAggregator;
//# sourceMappingURL=MultiProtocolFlashLoanAggregator.d.ts.map