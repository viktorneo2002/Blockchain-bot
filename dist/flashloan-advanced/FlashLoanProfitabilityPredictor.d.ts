import { Connection, PublicKey } from '@solana/web3.js';
import BN from 'bn.js';
import { EventEmitter } from 'events';
interface ArbitrageOpportunity {
    buyPool: PublicKey;
    sellPool: PublicKey;
    tokenA: PublicKey;
    tokenB: PublicKey;
    inputAmount: BN;
    expectedProfit: BN;
    profitPercentage: number;
    gasEstimate: number;
    confidence: number;
    marketImpact: number;
    executionTime: number;
    competitionScore: number;
}
interface FlashLoanParams {
    loanAmount: BN;
    loanToken: PublicKey;
    targetPool: PublicKey;
    repaymentAmount: BN;
    gasLimit: number;
    priority: number;
}
interface ProfitabilityMetrics {
    grossProfit: BN;
    netProfit: BN;
    roi: number;
    riskScore: number;
    successProbability: number;
    optimalLoanAmount: BN;
    breakEvenPrice: number;
    maxDrawdown: number;
    sharpeRatio: number;
}
interface MarketConditions {
    volatility: number;
    liquidityDepth: number;
    bidAskSpread: number;
    orderBookImbalance: number;
    recentTradeVolume: number;
    competitorActivity: number;
}
export declare class FlashLoanProfitabilityPredictor extends EventEmitter {
    private connection;
    private historicalData;
    private priceCache;
    private volatilityModel;
    private competitionAnalyzer;
    private readonly CONFIDENCE_THRESHOLD;
    private readonly MIN_PROFIT_BASIS_POINTS;
    private readonly MAX_MARKET_IMPACT;
    private readonly GAS_BUFFER_MULTIPLIER;
    private readonly PRICE_STALENESS_MS;
    private readonly VOLATILITY_WINDOW;
    private readonly COMPETITION_DECAY_RATE;
    constructor(connection: Connection);
    analyzeProfitability(opportunity: ArbitrageOpportunity): Promise<ProfitabilityMetrics>;
    private analyzeMarketConditions;
    private calculateGrossProfit;
    private calculateROI;
    private estimateGasCost;
    private calculateSlippage;
    private estimateMarketImpact;
    private calculateOptimalParameters;
    private simulatePricePoints;
    private findOptimalAmount;
    private simulateProfit;
    private calculateBreakEvenPrice;
    private calculateRiskMetrics;
    private calculateStandardDeviation;
    private calculateMaxDrawdown;
    private calculateRiskScore;
    private calculateSuccessProbability;
    private assessCompetition;
    private getComplexityMultiplier;
    private getCongestionMultiplier;
    private calculatePriorityFee;
    private calculateVolatility;
    private calculateVolatilityTrend;
    private analyzeLiquidityDepth;
    private getPoolLiquidity;
    private parsePoolLiquidity;
    private analyzeOrderBook;
    private getOrderBookDepth;
    private parseOrderBook;
    private getRecentTradeVolume;
    private getPriceHistory;
    private getHistoricalKey;
    updateHistoricalData(opportunity: ArbitrageOpportunity, result: {
        profit: number;
        gasUsed: number;
        success: boolean;
        conditions: MarketConditions;
    }): Promise<void>;
    isProfitable(metrics: ProfitabilityMetrics): boolean;
    getOptimalExecutionParams(opportunity: ArbitrageOpportunity, metrics: ProfitabilityMetrics): FlashLoanParams;
}
export interface PredictionResult {
    shouldExecute: boolean;
    metrics: ProfitabilityMetrics;
    executionParams: FlashLoanParams | null;
    confidence: number;
    reasoning: string;
}
export declare class ProfitabilityEngine {
    private predictor;
    private readonly MIN_VOLUME_THRESHOLD;
    private readonly MAX_POSITION_SIZE;
    private readonly SAFETY_MARGIN;
    constructor(connection: Connection);
    evaluateOpportunity(opportunity: ArbitrageOpportunity): Promise<PredictionResult>;
    private checkVolumeRequirements;
    private checkPositionSize;
    private performSafetyChecks;
    private generateReasoning;
    private getEmptyMetrics;
    recordExecution(opportunity: ArbitrageOpportunity, result: {
        profit: number;
        gasUsed: number;
        success: boolean;
    }): Promise<void>;
    getPredictor(): FlashLoanProfitabilityPredictor;
}
export declare const createProfitabilityPredictor: (connection: Connection) => ProfitabilityEngine;
export declare const calculateRequiredProfit: (loanAmount: BN, gasEstimate: number, minProfitBasisPoints?: number) => BN;
export declare const estimateOptimalTiming: (volatility: number, competitionScore: number, marketConditions: MarketConditions) => {
    executeIn: number;
    urgency: "high" | "medium" | "low";
};
export declare const validateFlashLoanParams: (params: FlashLoanParams) => {
    valid: boolean;
    errors: string[];
};
export default FlashLoanProfitabilityPredictor;
//# sourceMappingURL=FlashLoanProfitabilityPredictor.d.ts.map