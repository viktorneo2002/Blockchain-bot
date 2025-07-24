import { Connection } from '@solana/web3.js';
import { EventEmitter } from 'events';
interface LatencyMetrics {
    avgLatency: number;
    minLatency: number;
    maxLatency: number;
    p95Latency: number;
    p99Latency: number;
    successRate: number;
    timestamp: number;
}
interface CompetitorProfile {
    address: string;
    avgResponseTime: number;
    transactionFrequency: number;
    successRate: number;
    lastActivity: number;
    riskScore: number;
    patterns: TransactionPattern[];
    latencyHistory: number[];
}
interface TransactionPattern {
    type: 'arbitrage' | 'liquidation' | 'mev' | 'market_making';
    frequency: number;
    avgProfit: number;
    gasUsage: number;
    timing: number[];
}
interface NetworkLatencyData {
    rpcEndpoint: string;
    latency: number;
    timestamp: number;
    blockHeight: number;
}
export declare class CompetitorLatencyProfiler extends EventEmitter {
    private connection;
    private competitors;
    private latencyHistory;
    private networkLatencies;
    private monitoringActive;
    private updateInterval;
    private readonly MAX_HISTORY_SIZE;
    private readonly PROFILING_INTERVAL;
    private readonly COMPETITOR_TIMEOUT;
    constructor(connection: Connection);
    private initializeProfiler;
    private setupNetworkMonitoring;
    private measureNetworkLatency;
    profileCompetitor(address: string): Promise<CompetitorProfile>;
    private analyzeCompetitorBehavior;
    private calculateTransactionFrequency;
    private calculateSuccessRate;
    private identifyTransactionPatterns;
    private classifyTransaction;
    private isArbitrageTransaction;
    private isLiquidationTransaction;
    private isMEVTransaction;
    private isMarketMakingTransaction;
    private calculateRiskScore;
    private updateLatencyHistory;
    private calculateAverageLatency;
    getLatencyMetrics(address?: string): LatencyMetrics;
    getCompetitorRanking(): CompetitorProfile[];
    getFastestCompetitors(limit?: number): CompetitorProfile[];
    getCompetitorsByPattern(patternType: TransactionPattern['type']): CompetitorProfile[];
    startContinuousMonitoring(addresses: string[]): Promise<void>;
    private startContinuousProfiler;
    private cleanupStaleData;
    stopMonitoring(): void;
    getNetworkLatencyTrend(minutes?: number): NetworkLatencyData[];
    predictCompetitorBehavior(address: string): {
        nextTransactionTime: number;
        confidence: number;
        expectedPattern: TransactionPattern['type'] | null;
    };
    private calculateAverageTimeBetween;
    private calculateTimingVariance;
    getCompetitorLatencyAdvantage(address: string): {
        advantage: number;
        ranking: number;
        percentile: number;
    };
    identifyLatencyBottlenecks(): {
        networkBottlenecks: string[];
        competitorBottlenecks: string[];
        recommendations: string[];
    };
    getCompetitorHeatmap(): Map<string, number>;
    benchmarkAgainstCompetitors(testTransactions?: number): Promise<{
        ourPerformance: LatencyMetrics;
        competitorComparison: Array<{
            address: string;
            performance: LatencyMetrics;
            advantage: number;
        }>;
    }>;
    private calculateMetricsFromArray;
    exportCompetitorData(): {
        competitors: CompetitorProfile[];
        networkMetrics: LatencyMetrics;
        exportTimestamp: number;
    };
    importCompetitorData(data: {
        competitors: CompetitorProfile[];
        networkMetrics: LatencyMetrics;
        exportTimestamp: number;
    }): void;
    destroy(): void;
    getCompetitorInsights(address: string): {
        profile: CompetitorProfile | null;
        prediction: ReturnType<CompetitorLatencyProfiler['predictCompetitorBehavior']>;
        advantage: ReturnType<CompetitorLatencyProfiler['getCompetitorLatencyAdvantage']>;
        threatLevel: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL';
    };
    getMarketConditionImpact(): {
        volatilityImpact: number;
        competitorActivity: number;
        optimalTiming: number[];
        riskWindows: Array<{
            start: number;
            end: number;
            risk: number;
        }>;
    };
    private identifyOptimalTimingWindows;
    private identifyRiskWindows;
    getCompetitiveEdgeMetrics(): {
        overallRanking: number;
        speedAdvantage: number;
        successRateComparison: number;
        marketShareEstimate: number;
        improvementAreas: string[];
    };
    generateCompetitorReport(): {
        summary: {
            totalCompetitors: number;
            activeCompetitors: number;
            topThreat: string | null;
            avgMarketLatency: number;
        };
        topCompetitors: CompetitorProfile[];
        marketAnalysis: ReturnType<CompetitorLatencyProfiler['getMarketConditionImpact']>;
        ourPosition: ReturnType<CompetitorLatencyProfiler['getCompetitiveEdgeMetrics']>;
        recommendations: string[];
    };
}
export default CompetitorLatencyProfiler;
//# sourceMappingURL=CompetitorLatencyProfiler.d.ts.map