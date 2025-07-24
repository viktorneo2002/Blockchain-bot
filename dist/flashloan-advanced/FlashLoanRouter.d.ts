import { Connection, PublicKey, TransactionInstruction, Keypair, Commitment } from '@solana/web3.js';
import { BN } from '@project-serum/anchor';
interface FlashLoanProvider {
    name: string;
    programId: PublicKey;
    loanAccount: PublicKey;
    feeRate: number;
    maxLoanAmount: BN;
    priority: number;
}
interface FlashLoanParams {
    amount: BN;
    tokenMint: PublicKey;
    targetInstructions: TransactionInstruction[];
    repaymentStrategy: RepaymentStrategy;
    priorityFee?: number;
    slippageBps?: number;
}
interface RepaymentStrategy {
    type: 'FIXED' | 'PERCENTAGE' | 'PROFIT_SHARE';
    value: number;
}
export declare class FlashLoanRouter {
    private connection;
    private wallet;
    private providers;
    private lookupTableCache;
    private readonly MAX_RETRIES;
    private readonly SIMULATION_COMMITMENT;
    private readonly EXECUTION_COMMITMENT;
    private readonly PRIORITY_FEE_PERCENTILE;
    private readonly MAX_COMPUTE_UNITS;
    private readonly JITO_TIP_ACCOUNTS;
    constructor(connection: Connection, wallet: Keypair);
    private initializeProviders;
    executeFlashLoan(params: FlashLoanParams): Promise<string>;
    private findOptimalRoute;
    private calculateRouteScore;
    private evaluateRoute;
    private calculateRepaymentAmount;
    private buildFlashLoanTransaction;
    private createBorrowInstruction;
    private createRepayInstruction;
    private buildTestTransaction;
    private simulateTransaction;
    private estimateProfit;
    private calculateConfidence;
    private executeWithRetry;
    private sendSmartTransaction;
    private confirmTransaction;
    private getDynamicPriorityFee;
    private fetchLookupTables;
    private isLookupTableAddress;
    private getRandomJitoTipAccount;
    private exponentialBackoff;
    getProviderStats(): Promise<Map<string, any>>;
    healthCheck(): Promise<boolean>;
    updateProvider(name: string, updates: Partial<FlashLoanProvider>): void;
    clearLookupTableCache(): void;
    preflightChecks(params: FlashLoanParams): Promise<boolean>;
    getOptimalTiming(): {
        execute: boolean;
        reason: string;
    };
    getNetworkCongestion(): Promise<number>;
    formatAmount(amount: BN, decimals: number): string;
    emergencyWithdraw(tokenMint: PublicKey): Promise<string>;
}
export declare const DEFAULT_SLIPPAGE_BPS = 50;
export declare const DEFAULT_PRIORITY_FEE = 50000;
export declare const MIN_PROFIT_THRESHOLD: BN;
export declare class InsufficientLiquidityError extends Error {
    constructor(message: string);
}
export declare class FlashLoanError extends Error {
    totalProfit: BN;
    constructor(message: string);
    averageExecutionTime: number;
    providerStats: Map<string, {
        attempts: number;
        successes: number;
        failures: number;
        totalFees: BN;
    }>;
}
export interface FlashLoanConfig {
    maxRetries?: number;
    simulationCommitment?: Commitment;
    executionCommitment?: Commitment;
    priorityFeePercentile?: number;
    maxComputeUnits?: number;
    jitoTipPercentage?: number;
    emergencySafetyWallet?: PublicKey;
    enableMetrics?: boolean;
    enableJito?: boolean;
    customProviders?: FlashLoanProvider[];
}
export declare const ValidationUtils: {
    isValidAmount(amount: BN): boolean;
    isValidTokenMint(mint: PublicKey): boolean;
    validateParams(params: FlashLoanParams): void;
};
export declare class FlashLoanMonitor {
    private metrics;
    constructor();
    recordAttempt(provider: string): void;
    recordSuccess(provider: string, profit: BN, fee: BN, executionTime: number): void;
    recordFailure(provider: string): void;
    getMetrics(): FlashLoanMetrics;
    reset(): void;
}
export declare function calculateOptimalLoanAmount(availableLiquidity: BN, maxLoanAmount: BN, targetProfit: BN, feeRate: number): BN;
export declare function performSafetyChecks(connection: Connection, wallet: PublicKey): Promise<{
    safe: boolean;
    warnings: string[];
}>;
export declare class FlashLoanRouterFactory {
    private static instance;
    static create(connection: Connection, wallet: Keypair, config?: FlashLoanConfig): FlashLoanRouter;
    static destroy(): void;
}
export declare const VERSION = "1.0.0";
export declare const BUILD_DATE: string;
export declare const PERFORMANCE_CONFIG: {
    readonly PARALLEL_SIMULATIONS: 3;
    readonly CACHE_TTL_MS: 60000;
    readonly MIN_PROFIT_USD: 1;
    readonly GAS_BUFFER_MULTIPLIER: 1.1;
    readonly BLOCKHASH_REFRESH_INTERVAL: 30000;
};
export declare function initialize(connection: Connection): Promise<void>;
export declare function cleanup(): Promise<void>;
export default FlashLoanRouter;
export declare function isFlashLoanError(error: unknown): error is FlashLoanError;
export declare function isInsufficientLiquidityError(error: unknown): error is InsufficientLiquidityError;
export declare const MODULE_READY = true;
interface FlashLoanMetrics {
    totalProfit: BN;
    averageExecutionTime: number;
    totalAttempts: number;
    successfulLoans: number;
    failedLoans: number;
    providerStats: Map<string, {
        attempts: number;
        successes: number;
        failures: number;
        totalFees: BN;
    }>;
}
//# sourceMappingURL=FlashLoanRouter.d.ts.map