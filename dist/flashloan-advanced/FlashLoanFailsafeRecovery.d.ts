import { Connection, PublicKey, Keypair } from '@solana/web3.js';
import { BN } from '@project-serum/anchor';
interface RecoveryConfig {
    maxRetries: number;
    retryDelayMs: number;
    emergencyWallet: PublicKey;
    priorityFeePercentile: number;
    maxPriorityFeeLamports: number;
    circuitBreakerThreshold: number;
    recoveryTimeout: number;
    minBalanceThreshold: BN;
}
interface FlashLoanPosition {
    tokenMint: PublicKey;
    amount: BN;
    borrowedFrom: PublicKey;
    repaymentAccount: PublicKey;
    timestamp: number;
    txSignature?: string;
}
interface RecoveryState {
    isRecovering: boolean;
    failureCount: number;
    lastFailureTimestamp: number;
    recoveredPositions: Map<string, FlashLoanPosition>;
    pendingPositions: Map<string, FlashLoanPosition>;
}
export declare class FlashLoanFailsafeRecovery {
    private connection;
    private wallet;
    private config;
    private recoveryState;
    private circuitBreakerActive;
    private healthCheckInterval;
    private lookupTableCache;
    constructor(connection: Connection, wallet: Keypair, config?: Partial<RecoveryConfig>);
    private emergencyFundTransfer;
    private getPositionKey;
    private getAvailableTokensForSwap;
    private calculateSwapAmount;
    private getAllTokenAccounts;
    initializeRecoverySystem(): Promise<void>;
    private validateWalletBalance;
    private startHealthMonitoring;
    private performHealthCheck;
    private activateCircuitBreaker;
    recoverFlashLoanPosition(position: FlashLoanPosition): Promise<boolean>;
    private executeRecoveryStrategy;
    private attemptDirectRepayment;
    private attemptLiquidation;
    private attemptEmergencySwap;
    private attemptPartialRepayment;
    private emergencyPositionExit;
    private retryWithExponentialBackoff;
    private sendOptimizedTransaction;
    private confirmTransaction;
    private calculateOptimalPriorityFee;
    private estimateComputeUnits;
    private getTokenBalance;
    private findCollateralAccounts;
    private estimateTokenValue;
    private fetchTokenPrice;
    private fetchPythPrice;
    private fetchSwitchboardPrice;
    private fetchJupiterPrice;
    private createLiquidationInstruction;
    private createProtocolLiquidation;
    private createSwapInstruction;
    private createJupiterSwap;
    private createRaydiumSwap;
    private createOrcaSwap;
    private findRaydiumPool;
    private findOrcaPool;
    private getOrcaVault;
    private getOrcaTickArray;
    private getOrcaOracle;
    private loadLookupTables;
}
export { FlashLoanPosition, RecoveryConfig, RecoveryState };
//# sourceMappingURL=FlashLoanFailsafeRecovery.d.ts.map