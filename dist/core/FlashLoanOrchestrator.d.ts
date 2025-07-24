import { Connection, PublicKey, TransactionInstruction, Keypair } from '@solana/web3.js';
import { BN } from "@project-serum/anchor";
import { EventEmitter } from 'events';
export interface FlashLoanProvider {
    name: string;
    programId: PublicKey;
    maxLoanAmount: BN;
    feeRate: number;
    priority: number;
    isActive: boolean;
}
export interface FlashLoanRequest {
    tokenMint: PublicKey;
    amount: BN;
    targetInstructions: TransactionInstruction[];
    maxSlippage: number;
    priorityFee: number;
    timeout: number;
}
export interface FlashLoanResult {
    success: boolean;
    txSignature?: string;
    profit?: BN;
    gasUsed?: BN;
    provider?: string;
    error?: Error;
    timestamp: number;
}
export interface ProviderConfig {
    solend?: {
        programId: string;
        poolId: string;
    };
    portFinance?: {
        programId: string;
        marketId: string;
    };
    mango?: {
        programId: string;
        groupId: string;
    };
    custom?: Array<{
        name: string;
        programId: string;
        priority: number;
    }>;
}
export declare class FlashLoanOrchestrator extends EventEmitter {
    private connection;
    private wallet;
    private providers;
    private activeLoans;
    private readonly maxRetries;
    private readonly defaultTimeout;
    private readonly maxConcurrentLoans;
    private performanceMetrics;
    private circuitBreaker;
    constructor(connection: Connection, wallet: Keypair, providerConfig?: ProviderConfig);
    private initializeProviders;
    executeFlashLoan(request: FlashLoanRequest): Promise<FlashLoanResult>;
    private validateRequest;
    private selectOptimalProvider;
    private buildFlashLoanTransaction;
    private buildProviderInstructions;
    private buildSolendInstructions;
    private buildPortInstructions;
    private buildMangoInstructions;
    private buildGenericInstructions;
    private getOrCreateTokenAccount;
    private executeWithRetry;
    private calculateProfit;
    private updateMetrics;
    private getAveragePerformance;
    private handleProviderFailure;
    private getProviderFromError;
    private generateLoanId;
    private delay;
    healthCheck(): Promise<{
        healthy: boolean;
        providers: Array<{
            name: string;
            active: boolean;
            avgPerformance: number;
            circuitBreakerStatus: string;
        }>;
        activeLoans: number;
    }>;
    updateProviderStatus(providerName: string, isActive: boolean): void;
    resetCircuitBreaker(providerName?: string): void;
    getMetrics(): {
        totalLoansExecuted: number;
        averageExecutionTime: number;
        successRate: number;
        topProvider: string | null;
    };
    simulateFlashLoan(request: FlashLoanRequest): Promise<{
        feasible: boolean;
        estimatedProfit: BN;
        estimatedGas: BN;
        bestProvider: FlashLoanProvider | null;
        warnings: string[];
    }>;
    emergencyShutdown(): Promise<void>;
    addCustomProvider(name: string, programId: string, config: {
        maxLoanAmount: BN;
        feeRate: number;
        priority?: number;
    }): void;
    removeProvider(providerName: string): boolean;
    getProviderStats(providerName: string): {
        name: string;
        programId: string;
        maxLoanAmount: string;
        feeRate: number;
        priority: number;
        isActive: boolean;
        performance: {
            avgExecutionTime: number;
            totalExecutions: number;
            failures: number;
            lastFailure: number | null;
        };
    } | null;
    optimizeGasSettings(recentPriorityFees: number[]): Promise<{
        recommendedFee: number;
        confidence: number;
    }>;
    private calculateVariance;
    dispose(): void;
}
export default FlashLoanOrchestrator;
//# sourceMappingURL=FlashLoanOrchestrator.d.ts.map