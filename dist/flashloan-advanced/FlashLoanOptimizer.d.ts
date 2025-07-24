import { Connection, PublicKey, Transaction, Keypair } from '@solana/web3.js';
import BN from 'bn.js';
interface FlashLoanConfig {
    maxLoanAmount: BN;
    minProfitBps: number;
    maxSlippageBps: number;
    priorityFeePerCU: number;
    computeUnitLimit: number;
    maxRetries: number;
    confirmationTimeout: number;
}
interface ArbitrageRoute {
    inputMint: PublicKey;
    outputMint: PublicKey;
    inputAmount: BN;
    expectedOutput: BN;
    profit: BN;
    route: SwapLeg[];
    flashLoanProvider: FlashLoanProvider;
}
interface SwapLeg {
    protocol: string;
    programId: PublicKey;
    poolAddress: PublicKey;
    inputMint: PublicKey;
    outputMint: PublicKey;
    inputAmount: BN;
    minOutputAmount: BN;
    accounts: PublicKey[];
}
interface FlashLoanProvider {
    programId: PublicKey;
    poolAddress: PublicKey;
    feeAmount: BN;
    maxAmount: BN;
}
export declare class FlashLoanOptimizer {
    private connection;
    private wallet;
    private config;
    private flashLoanProviders;
    private lookupTableCache;
    private isRunning;
    constructor(connection: Connection, wallet: Keypair, config: FlashLoanConfig);
    initialize(): Promise<void>;
    private loadFlashLoanProviders;
    private loadLookupTables;
    findArbitrageOpportunities(tokenMints: PublicKey[]): Promise<ArbitrageRoute[]>;
    private findOptimalRoutes;
    private findDirectRoute;
    private findMultiHopRoute;
    private getQuote;
    private getRaydiumQuote;
    private getOrcaQuote;
    private getSerumQuote;
    private calculateSwapOutput;
    private calculateProfit;
    private isProfitable;
    executeArbitrage(route: ArbitrageRoute): Promise<string | null>;
    private buildArbitrageTransaction;
    private createFlashLoanBorrowInstruction;
    private createFlashLoanRepayInstruction;
    private createSwapInstruction;
    private createRaydiumSwapInstruction;
    private createOrcaSwapInstruction;
    private createSerumSwapInstruction;
    private sendOptimizedTransaction;
    private confirmTransactionWithTimeout;
    private findRaydiumPool;
    private findOrcaPool;
    private findSerumMarket;
    private getRaydiumPools;
    private getOrcaPools;
    private getSerumMarkets;
    private parseRaydiumPoolData;
    private parseOrcaPoolData;
    private getSerumMarketPrice;
    private parseSerumSlab;
    start(): Promise<void>;
    stop(): Promise<void>;
    getBalance(mint: PublicKey): Promise<BN>;
    monitorTransaction(signature: string): Promise<boolean>;
    private validateConfig;
    getOptimalLoanAmount(mint: PublicKey, route: SwapLeg[]): Promise<BN>;
    private simulateRoute;
    private simulateSwap;
    emergencyWithdraw(mint: PublicKey): Promise<void>;
    getStatus(): {
        isRunning: boolean;
        flashLoanProviders: number;
        lookupTables: number;
    };
    updatePriorityFee(): Promise<void>;
    private checkHealthFactors;
    optimizeTransaction(tx: Transaction): Promise<Transaction>;
    private logArbitrageMetrics;
}
export default FlashLoanOptimizer;
//# sourceMappingURL=FlashLoanOptimizer.d.ts.map