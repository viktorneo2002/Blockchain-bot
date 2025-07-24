"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CompetitorLatencyProfiler = void 0;
const web3_js_1 = require("@solana/web3.js");
const events_1 = require("events");
const perf_hooks_1 = require("perf_hooks");
class CompetitorLatencyProfiler extends events_1.EventEmitter {
    constructor(connection) {
        super();
        this.competitors = new Map();
        this.latencyHistory = new Map();
        this.networkLatencies = [];
        this.monitoringActive = false;
        this.updateInterval = null;
        this.MAX_HISTORY_SIZE = 1000;
        this.PROFILING_INTERVAL = 100;
        this.COMPETITOR_TIMEOUT = 30000;
        this.connection = connection;
        this.initializeProfiler();
    }
    initializeProfiler() {
        this.setupNetworkMonitoring();
        this.startContinuousProfiler();
    }
    setupNetworkMonitoring() {
        setInterval(async () => {
            await this.measureNetworkLatency();
        }, this.PROFILING_INTERVAL);
    }
    async measureNetworkLatency() {
        const startTime = perf_hooks_1.performance.now();
        try {
            const slot = await this.connection.getSlot('confirmed');
            const endTime = perf_hooks_1.performance.now();
            const latency = endTime - startTime;
            this.networkLatencies.push({
                rpcEndpoint: this.connection.rpcEndpoint,
                latency,
                timestamp: Date.now(),
                blockHeight: slot
            });
            if (this.networkLatencies.length > this.MAX_HISTORY_SIZE) {
                this.networkLatencies.shift();
            }
            this.emit('networkLatency', { latency, slot });
        }
        catch (error) {
            this.emit('error', { type: 'network_latency', error });
        }
    }
    async profileCompetitor(address) {
        const startTime = perf_hooks_1.performance.now();
        try {
            const pubkey = new web3_js_1.PublicKey(address);
            const accountInfo = await this.connection.getAccountInfo(pubkey);
            const signatures = await this.connection.getSignaturesForAddress(pubkey, { limit: 100 });
            const responseTime = perf_hooks_1.performance.now() - startTime;
            const profile = await this.analyzeCompetitorBehavior(address, signatures, responseTime);
            this.competitors.set(address, profile);
            this.updateLatencyHistory(address, responseTime);
            this.emit('competitorProfiled', profile);
            return profile;
        }
        catch (error) {
            this.emit('error', { type: 'competitor_profiling', address, error });
            throw error;
        }
    }
    async analyzeCompetitorBehavior(address, signatures, responseTime) {
        const now = Date.now();
        const recentSignatures = signatures.filter(sig => now - (sig.blockTime * 1000) < 3600000 // Last hour
        );
        const transactionFrequency = this.calculateTransactionFrequency(recentSignatures);
        const successRate = this.calculateSuccessRate(recentSignatures);
        const patterns = await this.identifyTransactionPatterns(recentSignatures);
        const riskScore = this.calculateRiskScore(transactionFrequency, successRate, patterns);
        const existingProfile = this.competitors.get(address);
        const latencyHistory = existingProfile?.latencyHistory || [];
        latencyHistory.push(responseTime);
        if (latencyHistory.length > this.MAX_HISTORY_SIZE) {
            latencyHistory.shift();
        }
        return {
            address,
            avgResponseTime: this.calculateAverageLatency(latencyHistory),
            transactionFrequency,
            successRate,
            lastActivity: recentSignatures[0]?.blockTime * 1000 || 0,
            riskScore,
            patterns,
            latencyHistory
        };
    }
    calculateTransactionFrequency(signatures) {
        if (signatures.length < 2)
            return 0;
        const timeSpan = signatures[0].blockTime - signatures[signatures.length - 1].blockTime;
        return timeSpan > 0 ? signatures.length / (timeSpan / 3600) : 0;
    }
    calculateSuccessRate(signatures) {
        if (signatures.length === 0)
            return 0;
        const successful = signatures.filter(sig => !sig.err).length;
        return successful / signatures.length;
    }
    async identifyTransactionPatterns(signatures) {
        const patterns = new Map();
        for (const sig of signatures) {
            try {
                const tx = await this.connection.getParsedTransaction(sig.signature);
                if (!tx)
                    continue;
                const pattern = this.classifyTransaction(tx);
                if (pattern) {
                    const existing = patterns.get(pattern.type) || {
                        type: pattern.type,
                        frequency: 0,
                        avgProfit: 0,
                        gasUsage: 0,
                        timing: []
                    };
                    existing.frequency++;
                    existing.timing.push(sig.blockTime);
                    existing.gasUsage += tx.meta?.fee || 0;
                    patterns.set(pattern.type, existing);
                }
            }
            catch (error) {
                continue;
            }
        }
        return Array.from(patterns.values());
    }
    classifyTransaction(tx) {
        const instructions = tx.transaction.message.instructions;
        if (this.isArbitrageTransaction(instructions)) {
            return { type: 'arbitrage' };
        }
        else if (this.isLiquidationTransaction(instructions)) {
            return { type: 'liquidation' };
        }
        else if (this.isMEVTransaction(instructions)) {
            return { type: 'mev' };
        }
        else if (this.isMarketMakingTransaction(instructions)) {
            return { type: 'market_making' };
        }
        return null;
    }
    isArbitrageTransaction(instructions) {
        const swapCount = instructions.filter(ix => ix.programId && (ix.programId.toString().includes('SwaPpA9LAaLfeLi3a68M4DjnLqgtticKg6CnyNwgAC8') ||
            ix.programId.toString().includes('9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM'))).length;
        return swapCount >= 2;
    }
    isLiquidationTransaction(instructions) {
        return instructions.some(ix => ix.data && ix.data.toString().includes('liquidate'));
    }
    isMEVTransaction(instructions) {
        return instructions.length > 5 &&
            instructions.some(ix => ix.programId?.toString().includes('MEV'));
    }
    isMarketMakingTransaction(instructions) {
        const orderInstructions = instructions.filter(ix => ix.data && (ix.data.toString().includes('place') ||
            ix.data.toString().includes('cancel')));
        return orderInstructions.length >= 2;
    }
    calculateRiskScore(frequency, successRate, patterns) {
        let score = 0;
        // High frequency increases risk
        score += Math.min(frequency / 100, 1) * 30;
        // High success rate increases risk
        score += successRate * 40;
        // Pattern complexity increases risk
        score += patterns.length * 10;
        // MEV and arbitrage patterns are higher risk
        const highRiskPatterns = patterns.filter(p => p.type === 'mev' || p.type === 'arbitrage');
        score += highRiskPatterns.length * 20;
        return Math.min(score, 100);
    }
    updateLatencyHistory(address, latency) {
        const history = this.latencyHistory.get(address) || [];
        history.push(latency);
        if (history.length > this.MAX_HISTORY_SIZE) {
            history.shift();
        }
        this.latencyHistory.set(address, history);
    }
    calculateAverageLatency(latencies) {
        if (latencies.length === 0)
            return 0;
        return latencies.reduce((sum, lat) => sum + lat, 0) / latencies.length;
    }
    getLatencyMetrics(address) {
        const latencies = address
            ? this.latencyHistory.get(address) || []
            : this.networkLatencies.map(nl => nl.latency);
        if (latencies.length === 0) {
            return {
                avgLatency: 0,
                minLatency: 0,
                maxLatency: 0,
                p95Latency: 0,
                p99Latency: 0,
                successRate: 0,
                timestamp: Date.now()
            };
        }
        const sorted = [...latencies].sort((a, b) => a - b);
        const p95Index = Math.floor(sorted.length * 0.95);
        const p99Index = Math.floor(sorted.length * 0.99);
        return {
            avgLatency: this.calculateAverageLatency(latencies),
            minLatency: Math.min(...latencies),
            maxLatency: Math.max(...latencies),
            p95Latency: sorted[p95Index] || 0,
            p99Latency: sorted[p99Index] || 0,
            successRate: 1.0, // Calculated separately for competitors
            timestamp: Date.now()
        };
    }
    getCompetitorRanking() {
        return Array.from(this.competitors.values())
            .sort((a, b) => b.riskScore - a.riskScore)
            .filter(profile => Date.now() - profile.lastActivity < this.COMPETITOR_TIMEOUT);
    }
    getFastestCompetitors(limit = 10) {
        return Array.from(this.competitors.values())
            .sort((a, b) => a.avgResponseTime - b.avgResponseTime)
            .slice(0, limit)
            .filter(profile => Date.now() - profile.lastActivity < this.COMPETITOR_TIMEOUT);
    }
    getCompetitorsByPattern(patternType) {
        return Array.from(this.competitors.values())
            .filter(profile => profile.patterns.some(pattern => pattern.type === patternType) &&
            Date.now() - profile.lastActivity < this.COMPETITOR_TIMEOUT)
            .sort((a, b) => b.riskScore - a.riskScore);
    }
    async startContinuousMonitoring(addresses) {
        this.monitoringActive = true;
        const monitorCompetitors = async () => {
            if (!this.monitoringActive)
                return;
            const promises = addresses.map(address => this.profileCompetitor(address).catch(error => this.emit('error', { type: 'monitoring', address, error })));
            await Promise.allSettled(promises);
            this.emit('monitoringCycle', {
                timestamp: Date.now(),
                competitorsMonitored: addresses.length,
                activeCompetitors: this.getCompetitorRanking().length
            });
        };
        // Initial profiling
        await monitorCompetitors();
        // Continuous monitoring
        this.updateInterval = setInterval(monitorCompetitors, this.PROFILING_INTERVAL);
    }
    startContinuousProfiler() {
        setInterval(() => {
            this.cleanupStaleData();
            this.emit('profilerStats', {
                totalCompetitors: this.competitors.size,
                activeCompetitors: this.getCompetitorRanking().length,
                networkLatency: this.getLatencyMetrics(),
                timestamp: Date.now()
            });
        }, 5000);
    }
    cleanupStaleData() {
        const now = Date.now();
        // Remove stale competitors
        for (const [address, profile] of this.competitors.entries()) {
            if (now - profile.lastActivity > this.COMPETITOR_TIMEOUT) {
                this.competitors.delete(address);
                this.latencyHistory.delete(address);
            }
        }
        // Trim network latency history
        if (this.networkLatencies.length > this.MAX_HISTORY_SIZE) {
            this.networkLatencies = this.networkLatencies.slice(-this.MAX_HISTORY_SIZE);
        }
    }
    stopMonitoring() {
        this.monitoringActive = false;
        if (this.updateInterval) {
            clearInterval(this.updateInterval);
            this.updateInterval = null;
        }
    }
    getNetworkLatencyTrend(minutes = 5) {
        const cutoff = Date.now() - (minutes * 60 * 1000);
        return this.networkLatencies.filter(data => data.timestamp > cutoff);
    }
    predictCompetitorBehavior(address) {
        const profile = this.competitors.get(address);
        if (!profile || profile.patterns.length === 0) {
            return { nextTransactionTime: 0, confidence: 0, expectedPattern: null };
        }
        const mostFrequentPattern = profile.patterns.reduce((prev, current) => prev.frequency > current.frequency ? prev : current);
        const avgTimeBetweenTx = this.calculateAverageTimeBetween(mostFrequentPattern.timing);
        const nextTransactionTime = profile.lastActivity + avgTimeBetweenTx;
        // Confidence based on pattern consistency and recent activity
        const timingVariance = this.calculateTimingVariance(mostFrequentPattern.timing);
        const recencyFactor = Math.max(0, 1 - (Date.now() - profile.lastActivity) / 300000); // 5 min decay
        const confidence = Math.min(0.95, (profile.successRate * 0.4) + (recencyFactor * 0.4) + ((1 - timingVariance) * 0.2));
        return {
            nextTransactionTime,
            confidence,
            expectedPattern: mostFrequentPattern.type
        };
    }
    calculateAverageTimeBetween(timings) {
        if (timings.length < 2)
            return 0;
        const intervals = [];
        for (let i = 1; i < timings.length; i++) {
            intervals.push((timings[i - 1] - timings[i]) * 1000); // Convert to ms
        }
        return intervals.reduce((sum, interval) => sum + interval, 0) / intervals.length;
    }
    calculateTimingVariance(timings) {
        if (timings.length < 3)
            return 1;
        const intervals = [];
        for (let i = 1; i < timings.length; i++) {
            intervals.push(timings[i - 1] - timings[i]);
        }
        const mean = intervals.reduce((sum, val) => sum + val, 0) / intervals.length;
        const variance = intervals.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / intervals.length;
        return Math.min(1, variance / (mean * mean)); // Coefficient of variation
    }
    getCompetitorLatencyAdvantage(address) {
        const profile = this.competitors.get(address);
        if (!profile) {
            return { advantage: 0, ranking: -1, percentile: 0 };
        }
        const allProfiles = Array.from(this.competitors.values())
            .filter(p => Date.now() - p.lastActivity < this.COMPETITOR_TIMEOUT)
            .sort((a, b) => a.avgResponseTime - b.avgResponseTime);
        const ranking = allProfiles.findIndex(p => p.address === address) + 1;
        const percentile = ranking > 0 ? (1 - (ranking - 1) / allProfiles.length) * 100 : 0;
        const networkAvgLatency = this.getLatencyMetrics().avgLatency;
        const advantage = networkAvgLatency - profile.avgResponseTime;
        return { advantage, ranking, percentile };
    }
    identifyLatencyBottlenecks() {
        const networkMetrics = this.getLatencyMetrics();
        const networkBottlenecks = [];
        const competitorBottlenecks = [];
        const recommendations = [];
        // Network analysis
        if (networkMetrics.p95Latency > 200) {
            networkBottlenecks.push('High P95 network latency detected');
            recommendations.push('Consider switching to a faster RPC endpoint');
        }
        if (networkMetrics.maxLatency > 1000) {
            networkBottlenecks.push('Network spikes detected');
            recommendations.push('Implement connection pooling and retry logic');
        }
        // Competitor analysis
        const fastestCompetitors = this.getFastestCompetitors(5);
        const ourAvgLatency = networkMetrics.avgLatency;
        fastestCompetitors.forEach(competitor => {
            if (competitor.avgResponseTime < ourAvgLatency * 0.7) {
                competitorBottlenecks.push(`Competitor ${competitor.address.slice(0, 8)}... is ${Math.round(((ourAvgLatency - competitor.avgResponseTime) / ourAvgLatency) * 100)}% faster`);
            }
        });
        if (competitorBottlenecks.length > 0) {
            recommendations.push('Optimize transaction preparation and signing');
            recommendations.push('Consider geographic proximity to validators');
        }
        return { networkBottlenecks, competitorBottlenecks, recommendations };
    }
    getCompetitorHeatmap() {
        const heatmap = new Map();
        const now = Date.now();
        for (const [address, profile] of this.competitors.entries()) {
            if (now - profile.lastActivity > this.COMPETITOR_TIMEOUT)
                continue;
            // Heat score based on activity, speed, and success rate
            const activityScore = Math.max(0, 1 - (now - profile.lastActivity) / 300000); // 5 min decay
            const speedScore = Math.max(0, 1 - profile.avgResponseTime / 1000); // Normalize to 1s
            const successScore = profile.successRate;
            const riskScore = profile.riskScore / 100;
            const heatScore = (activityScore * 0.3) + (speedScore * 0.3) + (successScore * 0.2) + (riskScore * 0.2);
            heatmap.set(address, heatScore);
        }
        return heatmap;
    }
    async benchmarkAgainstCompetitors(testTransactions = 10) {
        const benchmarkResults = [];
        // Run benchmark transactions
        for (let i = 0; i < testTransactions; i++) {
            const startTime = perf_hooks_1.performance.now();
            try {
                await this.connection.getSlot('confirmed');
                const endTime = perf_hooks_1.performance.now();
                benchmarkResults.push(endTime - startTime);
            }
            catch (error) {
                // Skip failed attempts
                continue;
            }
            // Small delay between tests
            await new Promise(resolve => setTimeout(resolve, 50));
        }
        const ourPerformance = this.calculateMetricsFromArray(benchmarkResults);
        const competitorComparison = [];
        for (const [address, profile] of this.competitors.entries()) {
            if (Date.now() - profile.lastActivity > this.COMPETITOR_TIMEOUT)
                continue;
            const competitorMetrics = this.calculateMetricsFromArray(profile.latencyHistory);
            const advantage = competitorMetrics.avgLatency - ourPerformance.avgLatency;
            competitorComparison.push({
                address,
                performance: competitorMetrics,
                advantage
            });
        }
        competitorComparison.sort((a, b) => a.performance.avgLatency - b.performance.avgLatency);
        return { ourPerformance, competitorComparison };
    }
    calculateMetricsFromArray(latencies) {
        if (latencies.length === 0) {
            return {
                avgLatency: 0,
                minLatency: 0,
                maxLatency: 0,
                p95Latency: 0,
                p99Latency: 0,
                successRate: 0,
                timestamp: Date.now()
            };
        }
        const sorted = [...latencies].sort((a, b) => a - b);
        const p95Index = Math.floor(sorted.length * 0.95);
        const p99Index = Math.floor(sorted.length * 0.99);
        return {
            avgLatency: latencies.reduce((sum, lat) => sum + lat, 0) / latencies.length,
            minLatency: Math.min(...latencies),
            maxLatency: Math.max(...latencies),
            p95Latency: sorted[p95Index] || 0,
            p99Latency: sorted[p99Index] || 0,
            successRate: 1.0,
            timestamp: Date.now()
        };
    }
    exportCompetitorData() {
        return {
            competitors: Array.from(this.competitors.values()),
            networkMetrics: this.getLatencyMetrics(),
            exportTimestamp: Date.now()
        };
    }
    importCompetitorData(data) {
        // Only import recent data (within last hour)
        if (Date.now() - data.exportTimestamp > 3600000) {
            this.emit('warning', 'Imported data is stale, skipping import');
            return;
        }
        data.competitors.forEach(profile => {
            this.competitors.set(profile.address, profile);
            this.latencyHistory.set(profile.address, profile.latencyHistory);
        });
        this.emit('dataImported', {
            competitorsImported: data.competitors.length,
            timestamp: Date.now()
        });
    }
    destroy() {
        this.stopMonitoring();
        this.competitors.clear();
        this.latencyHistory.clear();
        this.networkLatencies.length = 0;
        this.removeAllListeners();
    }
    getCompetitorInsights(address) {
        const profile = this.competitors.get(address);
        if (!profile) {
            return {
                profile: null,
                prediction: { nextTransactionTime: 0, confidence: 0, expectedPattern: null },
                advantage: { advantage: 0, ranking: -1, percentile: 0 },
                threatLevel: 'LOW'
            };
        }
        const prediction = this.predictCompetitorBehavior(address);
        const advantage = this.getCompetitorLatencyAdvantage(address);
        let threatLevel = 'LOW';
        if (profile.riskScore > 80 && advantage.percentile > 90) {
            threatLevel = 'CRITICAL';
        }
        else if (profile.riskScore > 60 && advantage.percentile > 75) {
            threatLevel = 'HIGH';
        }
        else if (profile.riskScore > 40 && advantage.percentile > 50) {
            threatLevel = 'MEDIUM';
        }
        return { profile, prediction, advantage, threatLevel };
    }
    getMarketConditionImpact() {
        const now = Date.now();
        const hourAgo = now - 3600000;
        const recentActivity = Array.from(this.competitors.values())
            .filter(p => p.lastActivity > hourAgo)
            .length;
        const totalCompetitors = this.competitors.size;
        const competitorActivity = totalCompetitors > 0 ? recentActivity / totalCompetitors : 0;
        // Calculate volatility impact based on network latency variance
        const recentLatencies = this.networkLatencies
            .filter(nl => nl.timestamp > hourAgo)
            .map(nl => nl.latency);
        const volatilityImpact = recentLatencies.length > 0
            ? this.calculateTimingVariance(recentLatencies)
            : 0;
        // Identify optimal timing windows (low competitor activity)
        const optimalTiming = this.identifyOptimalTimingWindows();
        // Identify high-risk windows
        const riskWindows = this.identifyRiskWindows();
        return {
            volatilityImpact,
            competitorActivity,
            optimalTiming,
            riskWindows
        };
    }
    identifyOptimalTimingWindows() {
        const windows = [];
        const now = Date.now();
        const windowSize = 300000; // 5 minutes
        for (let i = 0; i < 12; i++) { // Check next hour in 5-min windows
            const windowStart = now + (i * windowSize);
            const windowEnd = windowStart + windowSize;
            const competitorsInWindow = Array.from(this.competitors.values())
                .filter(profile => {
                const prediction = this.predictCompetitorBehavior(profile.address);
                return prediction.nextTransactionTime >= windowStart &&
                    prediction.nextTransactionTime <= windowEnd &&
                    prediction.confidence > 0.5;
            }).length;
            if (competitorsInWindow < 3) { // Low competition window
                windows.push(windowStart);
            }
        }
        return windows;
    }
    identifyRiskWindows() {
        const windows = [];
        const now = Date.now();
        const windowSize = 300000; // 5 minutes
        for (let i = 0; i < 12; i++) {
            const windowStart = now + (i * windowSize);
            const windowEnd = windowStart + windowSize;
            const highRiskCompetitors = Array.from(this.competitors.values())
                .filter(profile => {
                const prediction = this.predictCompetitorBehavior(profile.address);
                return prediction.nextTransactionTime >= windowStart &&
                    prediction.nextTransactionTime <= windowEnd &&
                    prediction.confidence > 0.7 &&
                    profile.riskScore > 70;
            });
            if (highRiskCompetitors.length > 0) {
                const avgRisk = highRiskCompetitors.reduce((sum, p) => sum + p.riskScore, 0) / highRiskCompetitors.length;
                windows.push({
                    start: windowStart,
                    end: windowEnd,
                    risk: avgRisk
                });
            }
        }
        return windows.sort((a, b) => b.risk - a.risk);
    }
    getCompetitiveEdgeMetrics() {
        const ourMetrics = this.getLatencyMetrics();
        const allCompetitors = Array.from(this.competitors.values())
            .filter(p => Date.now() - p.lastActivity < this.COMPETITOR_TIMEOUT);
        if (allCompetitors.length === 0) {
            return {
                overallRanking: 1,
                speedAdvantage: 100,
                successRateComparison: 100,
                marketShareEstimate: 100,
                improvementAreas: []
            };
        }
        // Speed ranking
        const fasterCompetitors = allCompetitors.filter(p => p.avgResponseTime < ourMetrics.avgLatency).length;
        const speedRanking = fasterCompetitors + 1;
        const speedAdvantage = Math.max(0, 100 - (speedRanking / (allCompetitors.length + 1)) * 100);
        // Success rate comparison
        const avgCompetitorSuccessRate = allCompetitors.reduce((sum, p) => sum + p.successRate, 0) / allCompetitors.length;
        const successRateComparison = Math.min(100, (ourMetrics.successRate / avgCompetitorSuccessRate) * 100);
        // Market share estimate based on speed and activity
        const totalActivity = allCompetitors.reduce((sum, p) => sum + p.transactionFrequency, 0);
        const ourEstimatedActivity = Math.max(1, totalActivity / allCompetitors.length); // Assume average
        const marketShareEstimate = Math.min(100, (ourEstimatedActivity / (totalActivity + ourEstimatedActivity)) * 100);
        // Overall ranking
        const overallRanking = Math.ceil((speedRanking + (allCompetitors.length + 1 - (successRateComparison / 100 * allCompetitors.length))) / 2);
        // Improvement areas
        const improvementAreas = [];
        if (speedAdvantage < 75)
            improvementAreas.push('Optimize transaction speed');
        if (successRateComparison < 90)
            improvementAreas.push('Improve transaction success rate');
        if (marketShareEstimate < 20)
            improvementAreas.push('Increase transaction frequency');
        if (ourMetrics.p95Latency > 500)
            improvementAreas.push('Reduce latency spikes');
        return {
            overallRanking,
            speedAdvantage,
            successRateComparison,
            marketShareEstimate,
            improvementAreas
        };
    }
    generateCompetitorReport() {
        const activeCompetitors = this.getCompetitorRanking();
        const topCompetitors = activeCompetitors.slice(0, 10);
        const topThreat = activeCompetitors.length > 0 ? activeCompetitors[0].address : null;
        const marketAnalysis = this.getMarketConditionImpact();
        const ourPosition = this.getCompetitiveEdgeMetrics();
        const recommendations = [];
        if (ourPosition.overallRanking > 5) {
            recommendations.push('Focus on speed optimization to improve market position');
        }
        if (marketAnalysis.competitorActivity > 0.8) {
            recommendations.push('Market is highly competitive, consider timing optimization');
        }
        if (marketAnalysis.volatilityImpact > 0.5) {
            recommendations.push('High network volatility detected, implement adaptive strategies');
        }
        if (topCompetitors.length > 0 && topCompetitors[0].riskScore > 90) {
            recommendations.push('Critical threat detected, monitor top competitor closely');
        }
        recommendations.push(...ourPosition.improvementAreas);
        return {
            summary: {
                totalCompetitors: this.competitors.size,
                activeCompetitors: activeCompetitors.length,
                topThreat,
                avgMarketLatency: this.getLatencyMetrics().avgLatency
            },
            topCompetitors,
            marketAnalysis,
            ourPosition,
            recommendations: [...new Set(recommendations)] // Remove duplicates
        };
    }
}
exports.CompetitorLatencyProfiler = CompetitorLatencyProfiler;
exports.default = CompetitorLatencyProfiler;
//# sourceMappingURL=CompetitorLatencyProfiler.js.map