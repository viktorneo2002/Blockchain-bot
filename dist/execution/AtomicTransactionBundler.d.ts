import { VersionedTransaction, TransactionInstruction, PublicKey, Keypair, AddressLookupTableAccount, Commitment, SignatureResult } from '@solana/web3.js';
interface BundleConfig {
    maxRetries: number;
    simulationRetries: number;
    priorityFeeMultiplier: number;
    computeUnitLimit: number;
    jitoTipAmount: number;
    jitoBlockEngineUrl: string;
    rpcEndpoint: string;
    commitment: Commitment;
    skipPreflight: boolean;
    preflightCommitment: Commitment;
    maxSignatureWaitTime: number;
}
interface TransactionBundle {
    payer: PublicKey;
    instructions: TransactionInstruction[];
    signers: Keypair[];
    lookupTables?: AddressLookupTableAccount[];
    computeUnitLimit?: number;
    priorityFee?: number;
}
interface BundleResult {
    success: boolean;
    signatures?: string[];
    error?: Error;
    simulationLogs?: string[];
    slot?: number;
}
export declare class AtomicTransactionBundler {
    private connection;
    private jitoConnection;
    private config;
    private recentBlockhash;
    private lastBlockhashFetch;
    private blockHashCache;
    constructor(config?: Partial<BundleConfig>);
    executeAtomicBundle(bundles: TransactionBundle[]): Promise<BundleResult>;
    private buildVersionedTransactions;
    private createPriorityFeeInstruction;
    private createComputeUnitInstruction;
    private calculateDynamicPriorityFee;
    private simulateTransactions;
    private validateSimulations;
    private extractSimulationLogs;
    private createJitoBundle;
    private compileInstruction;
    private findAccountIndex;
    private getJitoTipAccount;
    private createJitoTipInstruction;
    private sendJitoBundle;
    private submitBundleToJito;
    private confirmBundle;
    private getRecentBlockhash;
    buildOptimizedTransaction(instructions: TransactionInstruction[], payerKey: PublicKey, signers: Keypair[], lookupTables?: AddressLookupTableAccount[]): Promise<VersionedTransaction>;
    private estimateComputeUnits;
    executeWithRetry<T>(operation: () => Promise<T>, retries?: number): Promise<T>;
    private isRetryableError;
    bundleAndExecute(transactionSets: {
        instructions: TransactionInstruction[];
        signers: Keypair[];
        computeLimit?: number;
        priorityFee?: number;
    }[]): Promise<BundleResult>;
    validateBundleAccounts(bundles: TransactionBundle[]): Promise<boolean>;
    private sleep;
    updateConfig(newConfig: Partial<BundleConfig>): void;
    getConfig(): Readonly<BundleConfig>;
    healthCheck(): Promise<boolean>;
    getTransactionCost(instructions: TransactionInstruction[], priorityFee?: number): Promise<number>;
    clearBlockhashCache(): void;
    monitorBundleStatus(signatures: string[]): Promise<Map<string, SignatureResult | null>>;
    getOptimalSlot(): Promise<number>;
    private validateTransactionSize;
    preBundleValidation(bundles: TransactionBundle[]): Promise<{
        valid: boolean;
        errors: string[];
    }>;
    getConnectionStats(): {
        mainnet: string;
        jito: string;
        cacheSize: number;
    };
}
export default AtomicTransactionBundler;
//# sourceMappingURL=AtomicTransactionBundler.d.ts.map