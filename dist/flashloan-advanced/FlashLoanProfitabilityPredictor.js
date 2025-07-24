"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.validateFlashLoanParams = exports.estimateOptimalTiming = exports.calculateRequiredProfit = exports.createProfitabilityPredictor = exports.ProfitabilityEngine = exports.FlashLoanProfitabilityPredictor = void 0;
const bn_js_1 = __importDefault(require("bn.js"));
const events_1 = require("events");
class FlashLoanProfitabilityPredictor extends events_1.EventEmitter {
    constructor(connection) {
        super();
        this.CONFIDENCE_THRESHOLD = 0.85;
        this.MIN_PROFIT_BASIS_POINTS = 15;
        this.MAX_MARKET_IMPACT = 0.003;
        this.GAS_BUFFER_MULTIPLIER = 1.25;
        this.PRICE_STALENESS_MS = 500;
        this.VOLATILITY_WINDOW = 3600000;
        this.COMPETITION_DECAY_RATE = 0.95;
        this.connection = connection;
        this.historicalData = new Map();
        this.priceCache = new Map();
        this.volatilityModel = new VolatilityPredictor();
        this.competitionAnalyzer = new CompetitionAnalyzer();
    }
    async analyzeProfitability(opportunity) {
        const startTime = Date.now();
        const [marketConditions, competitionScore, optimalParams, riskMetrics] = await Promise.all([
            this.analyzeMarketConditions(opportunity),
            this.assessCompetition(opportunity),
            this.calculateOptimalParameters(opportunity),
            this.calculateRiskMetrics(opportunity)
        ]);
        const gasEstimate = await this.estimateGasCost(opportunity, marketConditions);
        const slippage = this.calculateSlippage(opportunity, marketConditions);
        const marketImpact = this.estimateMarketImpact(opportunity, marketConditions);
        const grossProfit = this.calculateGrossProfit(opportunity, slippage, marketImpact);
        const netProfit = grossProfit.sub(new bn_js_1.default(gasEstimate));
        const roi = this.calculateROI(netProfit, opportunity.inputAmount);
        const successProbability = this.calculateSuccessProbability(opportunity, marketConditions, competitionScore, riskMetrics);
        const metrics = {
            grossProfit,
            netProfit,
            roi,
            riskScore: riskMetrics.score,
            successProbability,
            optimalLoanAmount: optimalParams.amount,
            breakEvenPrice: optimalParams.breakEven,
            maxDrawdown: riskMetrics.maxDrawdown,
            sharpeRatio: riskMetrics.sharpeRatio
        };
        this.emit('profitability-analyzed', {
            opportunity,
            metrics,
            executionTime: Date.now() - startTime
        });
        return metrics;
    }
    async analyzeMarketConditions(opportunity) {
        const [volatility, liquidityDepth, orderBook, recentVolume, competitorActivity] = await Promise.all([
            this.calculateVolatility(opportunity.tokenA, opportunity.tokenB),
            this.analyzeLiquidityDepth(opportunity.buyPool, opportunity.sellPool),
            this.analyzeOrderBook(opportunity.buyPool, opportunity.sellPool),
            this.getRecentTradeVolume(opportunity.tokenA, opportunity.tokenB),
            this.competitionAnalyzer.getActivityScore(opportunity)
        ]);
        return {
            volatility: volatility.current,
            liquidityDepth,
            bidAskSpread: orderBook.spread,
            orderBookImbalance: orderBook.imbalance,
            recentTradeVolume: recentVolume,
            competitorActivity
        };
    }
    calculateGrossProfit(opportunity, slippage, marketImpact) {
        const adjustedProfit = opportunity.expectedProfit.toNumber() *
            (1 - slippage - marketImpact);
        return new bn_js_1.default(Math.floor(adjustedProfit));
    }
    calculateROI(netProfit, inputAmount) {
        if (inputAmount.isZero())
            return 0;
        return netProfit.mul(new bn_js_1.default(10000)).div(inputAmount).toNumber() / 100;
    }
    async estimateGasCost(opportunity, conditions) {
        const baseGas = 5000;
        const complexityMultiplier = this.getComplexityMultiplier(opportunity);
        const congestionMultiplier = await this.getCongestionMultiplier();
        const priorityFee = this.calculatePriorityFee(opportunity, conditions);
        const totalGas = Math.ceil(baseGas * complexityMultiplier * congestionMultiplier * this.GAS_BUFFER_MULTIPLIER) + priorityFee;
        return totalGas;
    }
    calculateSlippage(opportunity, conditions) {
        const baseSlippage = 0.001;
        const volumeImpact = opportunity.inputAmount.toNumber() / conditions.liquidityDepth;
        const volatilityImpact = conditions.volatility * 0.1;
        const spreadImpact = conditions.bidAskSpread * 0.5;
        return Math.min(baseSlippage + volumeImpact + volatilityImpact + spreadImpact, 0.02);
    }
    estimateMarketImpact(opportunity, conditions) {
        const tradeSize = opportunity.inputAmount.toNumber();
        const liquidityDepth = conditions.liquidityDepth;
        const orderBookImbalance = conditions.orderBookImbalance;
        const sizeImpact = Math.pow(tradeSize / liquidityDepth, 1.5);
        const imbalanceMultiplier = 1 + Math.abs(orderBookImbalance) * 0.5;
        return Math.min(sizeImpact * imbalanceMultiplier, this.MAX_MARKET_IMPACT);
    }
    async calculateOptimalParameters(opportunity) {
        const pricePoints = await this.simulatePricePoints(opportunity);
        const optimalAmount = this.findOptimalAmount(pricePoints, opportunity);
        const breakEvenPrice = this.calculateBreakEvenPrice(opportunity, optimalAmount);
        return { amount: optimalAmount, breakEven: breakEvenPrice };
    }
    async simulatePricePoints(opportunity) {
        const baseAmount = opportunity.inputAmount;
        const points = [];
        for (let multiplier = 0.1; multiplier <= 3.0; multiplier += 0.1) {
            const amount = baseAmount.mul(new bn_js_1.default(Math.floor(multiplier * 100))).div(new bn_js_1.default(100));
            const profit = await this.simulateProfit(opportunity, amount);
            points.push({ amount, profit });
        }
        return points;
    }
    findOptimalAmount(pricePoints, opportunity) {
        let maxProfit = new bn_js_1.default(0);
        let optimalAmount = opportunity.inputAmount;
        for (const point of pricePoints) {
            if (point.profit.gt(maxProfit)) {
                maxProfit = point.profit;
                optimalAmount = point.amount;
            }
        }
        return optimalAmount;
    }
    async simulateProfit(opportunity, amount) {
        const marketConditions = await this.analyzeMarketConditions({
            ...opportunity,
            inputAmount: amount
        });
        const slippage = this.calculateSlippage({ ...opportunity, inputAmount: amount }, marketConditions);
        const marketImpact = this.estimateMarketImpact({ ...opportunity, inputAmount: amount }, marketConditions);
        const gasEstimate = await this.estimateGasCost({ ...opportunity, inputAmount: amount }, marketConditions);
        const grossProfit = this.calculateGrossProfit({ ...opportunity, inputAmount: amount }, slippage, marketImpact);
        return grossProfit.sub(new bn_js_1.default(gasEstimate));
    }
    calculateBreakEvenPrice(opportunity, amount) {
        const gasCost = opportunity.gasEstimate;
        const totalCost = amount.add(new bn_js_1.default(gasCost));
        const requiredReturn = totalCost.mul(new bn_js_1.default(10015)).div(new bn_js_1.default(10000));
        return requiredReturn.toNumber() / amount.toNumber();
    }
    async calculateRiskMetrics(opportunity) {
        const historicalKey = this.getHistoricalKey(opportunity);
        const history = this.historicalData.get(historicalKey) || [];
        const returns = history.map(h => h.profit);
        const volatility = this.calculateStandardDeviation(returns);
        const averageReturn = returns.reduce((a, b) => a + b, 0) / returns.length || 0;
        const maxDrawdown = this.calculateMaxDrawdown(returns);
        const sharpeRatio = volatility > 0 ? (averageReturn - 0.02) / volatility : 0;
        const riskScore = this.calculateRiskScore(volatility, maxDrawdown, sharpeRatio, opportunity);
        return { score: riskScore, maxDrawdown, sharpeRatio };
    }
    calculateStandardDeviation(values) {
        if (values.length === 0)
            return 0;
        const mean = values.reduce((a, b) => a + b, 0) / values.length;
        const variance = values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / values.length;
        return Math.sqrt(variance);
    }
    calculateMaxDrawdown(returns) {
        if (returns.length === 0)
            return 0;
        let peak = returns[0];
        let maxDrawdown = 0;
        let cumulative = 0;
        for (const ret of returns) {
            cumulative += ret;
            if (cumulative > peak) {
                peak = cumulative;
            }
            const drawdown = (peak - cumulative) / Math.abs(peak);
            maxDrawdown = Math.max(maxDrawdown, drawdown);
        }
        return maxDrawdown;
    }
    calculateRiskScore(volatility, maxDrawdown, sharpeRatio, opportunity) {
        const volScore = Math.max(0, 1 - volatility * 10);
        const ddScore = Math.max(0, 1 - maxDrawdown * 2);
        const sharpeScore = Math.min(1, Math.max(0, sharpeRatio / 3));
        const sizeScore = 1 - Math.min(1, opportunity.inputAmount.toNumber() / 1000000);
        return (volScore * 0.3 + ddScore * 0.3 + sharpeScore * 0.3 + sizeScore * 0.1);
    }
    calculateSuccessProbability(opportunity, conditions, competitionScore, riskMetrics) {
        const profitScore = Math.min(1, opportunity.profitPercentage / 5);
        const liquidityScore = Math.min(1, conditions.liquidityDepth / 1000000);
        const volatilityPenalty = Math.max(0, 1 - conditions.volatility * 5);
        const competitionPenalty = Math.max(0, 1 - competitionScore);
        const riskAdjustment = riskMetrics.score;
        const spreadPenalty = Math.max(0, 1 - conditions.bidAskSpread * 100);
        const weights = {
            profit: 0.25,
            liquidity: 0.20,
            volatility: 0.15,
            competition: 0.15,
            risk: 0.15,
            spread: 0.10
        };
        const probability = profitScore * weights.profit +
            liquidityScore * weights.liquidity +
            volatilityPenalty * weights.volatility +
            competitionPenalty * weights.competition +
            riskAdjustment * weights.risk +
            spreadPenalty * weights.spread;
        return Math.max(0, Math.min(1, probability));
    }
    async assessCompetition(opportunity) {
        const recentActivity = await this.competitionAnalyzer.getRecentActivity(opportunity.buyPool, opportunity.sellPool);
        const decayedScore = recentActivity.reduce((score, activity) => {
            const age = Date.now() - activity.timestamp;
            const decay = Math.pow(this.COMPETITION_DECAY_RATE, age / 60000);
            return score + activity.intensity * decay;
        }, 0);
        return Math.min(1, decayedScore / 10);
    }
    getComplexityMultiplier(opportunity) {
        const baseComplexity = 1.0;
        const poolComplexity = 0.2;
        const tokenComplexity = 0.1;
        return baseComplexity + poolComplexity + tokenComplexity;
    }
    async getCongestionMultiplier() {
        try {
            const recentSlot = await this.connection.getSlot();
            const blockTime = await this.connection.getBlockTime(recentSlot);
            const recentBlocks = await this.connection.getBlocks(recentSlot - 150, recentSlot);
            const avgBlockTime = blockTime ? 400 : 450;
            const congestion = Math.min(2, Math.max(1, 450 / avgBlockTime));
            return congestion;
        }
        catch {
            return 1.5;
        }
    }
    calculatePriorityFee(opportunity, conditions) {
        const profitBasisPoints = opportunity.profitPercentage * 100;
        const competitionMultiplier = 1 + conditions.competitorActivity;
        const urgencyMultiplier = Math.min(2, Math.max(1, profitBasisPoints / 50));
        const basePriority = 1000;
        return Math.ceil(basePriority * competitionMultiplier * urgencyMultiplier);
    }
    async calculateVolatility(tokenA, tokenB) {
        const priceHistory = await this.getPriceHistory(tokenA, tokenB);
        if (priceHistory.length < 2) {
            return { current: 0.01, trend: 0 };
        }
        const returns = [];
        for (let i = 1; i < priceHistory.length; i++) {
            const ret = (priceHistory[i].price - priceHistory[i - 1].price) / priceHistory[i - 1].price;
            returns.push(ret);
        }
        const volatility = this.calculateStandardDeviation(returns);
        const trend = this.calculateVolatilityTrend(returns);
        return { current: volatility, trend };
    }
    calculateVolatilityTrend(returns) {
        if (returns.length < 10)
            return 0;
        const recentVol = this.calculateStandardDeviation(returns.slice(-5));
        const historicalVol = this.calculateStandardDeviation(returns.slice(0, -5));
        return historicalVol > 0 ? (recentVol - historicalVol) / historicalVol : 0;
    }
    async analyzeLiquidityDepth(buyPool, sellPool) {
        const [buyLiquidity, sellLiquidity] = await Promise.all([
            this.getPoolLiquidity(buyPool),
            this.getPoolLiquidity(sellPool)
        ]);
        return Math.min(buyLiquidity, sellLiquidity);
    }
    async getPoolLiquidity(pool) {
        try {
            const accountInfo = await this.connection.getAccountInfo(pool);
            if (!accountInfo)
                return 0;
            const liquidity = this.parsePoolLiquidity(accountInfo.data);
            return liquidity;
        }
        catch {
            return 0;
        }
    }
    parsePoolLiquidity(data) {
        if (data.length < 72)
            return 0;
        const liquidityBytes = data.slice(64, 72);
        const liquidity = new bn_js_1.default(liquidityBytes, 'le');
        return liquidity.toNumber() / 1e9;
    }
    async analyzeOrderBook(buyPool, sellPool) {
        const [buyOrders, sellOrders] = await Promise.all([
            this.getOrderBookDepth(buyPool),
            this.getOrderBookDepth(sellPool)
        ]);
        const spread = Math.abs(buyOrders.bestAsk - sellOrders.bestBid) / sellOrders.bestBid;
        const imbalance = (buyOrders.totalBids - sellOrders.totalAsks) /
            (buyOrders.totalBids + sellOrders.totalAsks);
        return { spread, imbalance };
    }
    async getOrderBookDepth(pool) {
        try {
            const accountInfo = await this.connection.getAccountInfo(pool);
            if (!accountInfo) {
                return { bestBid: 0, bestAsk: 0, totalBids: 0, totalAsks: 0 };
            }
            return this.parseOrderBook(accountInfo.data);
        }
        catch {
            return { bestBid: 0, bestAsk: 0, totalBids: 0, totalAsks: 0 };
        }
    }
    parseOrderBook(data) {
        if (data.length < 200) {
            return { bestBid: 0, bestAsk: 0, totalBids: 0, totalAsks: 0 };
        }
        const bestBid = new bn_js_1.default(data.slice(72, 80), 'le').toNumber() / 1e9;
        const bestAsk = new bn_js_1.default(data.slice(80, 88), 'le').toNumber() / 1e9;
        const totalBids = new bn_js_1.default(data.slice(88, 96), 'le').toNumber() / 1e9;
        const totalAsks = new bn_js_1.default(data.slice(96, 104), 'le').toNumber() / 1e9;
        return { bestBid, bestAsk, totalBids, totalAsks };
    }
    async getRecentTradeVolume(tokenA, tokenB) {
        const key = `${tokenA.toString()}-${tokenB.toString()}`;
        const cached = this.priceCache.get(key);
        if (cached && Date.now() - cached.lastUpdate < this.PRICE_STALENESS_MS) {
            return cached.volume24h;
        }
        return 0;
    }
    async getPriceHistory(tokenA, tokenB) {
        const key = `${tokenA.toString()}-${tokenB.toString()}`;
        const history = this.historicalData.get(key) || [];
        return history
            .filter(h => Date.now() - h.timestamp < this.VOLATILITY_WINDOW)
            .map(h => ({ price: h.profit, timestamp: h.timestamp }));
    }
    getHistoricalKey(opportunity) {
        return `${opportunity.tokenA.toString()}-${opportunity.tokenB.toString()}`;
    }
    async updateHistoricalData(opportunity, result) {
        const key = this.getHistoricalKey(opportunity);
        const history = this.historicalData.get(key) || [];
        history.push({
            timestamp: Date.now(),
            profit: result.profit,
            gasUsed: result.gasUsed,
            success: result.success,
            marketConditions: result.conditions
        });
        const maxHistory = 10000;
        if (history.length > maxHistory) {
            history.splice(0, history.length - maxHistory);
        }
        this.historicalData.set(key, history);
    }
    isProfitable(metrics) {
        return (metrics.netProfit.gt(new bn_js_1.default(0)) &&
            metrics.roi >= this.MIN_PROFIT_BASIS_POINTS / 100 &&
            metrics.successProbability >= this.CONFIDENCE_THRESHOLD &&
            metrics.riskScore >= 0.7);
    }
    getOptimalExecutionParams(opportunity, metrics) {
        const priorityMultiplier = Math.min(3, Math.max(1, metrics.roi / 2));
        const gasLimit = Math.ceil(opportunity.gasEstimate * this.GAS_BUFFER_MULTIPLIER);
        return {
            loanAmount: metrics.optimalLoanAmount,
            loanToken: opportunity.tokenA,
            targetPool: opportunity.buyPool,
            repaymentAmount: metrics.optimalLoanAmount.add(metrics.grossProfit),
            gasLimit,
            priority: Math.ceil(1000 * priorityMultiplier)
        };
    }
}
exports.FlashLoanProfitabilityPredictor = FlashLoanProfitabilityPredictor;
class VolatilityPredictor {
    constructor() {
        this.GARCH_ALPHA = 0.1;
        this.GARCH_BETA = 0.85;
        this.GARCH_OMEGA = 0.05;
    }
    predict(returns, horizon) {
        if (returns.length < 2)
            return 0.01;
        let variance = Math.pow(this.calculateStandardDeviation(returns), 2);
        for (let i = 0; i < horizon; i++) {
            variance = this.GARCH_OMEGA +
                this.GARCH_ALPHA * Math.pow(returns[returns.length - 1] || 0, 2) +
                this.GARCH_BETA * variance;
        }
        return Math.sqrt(variance);
    }
    calculateStandardDeviation(values) {
        if (values.length === 0)
            return 0;
        const mean = values.reduce((a, b) => a + b, 0) / values.length;
        const variance = values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / values.length;
        return Math.sqrt(variance);
    }
}
class CompetitionAnalyzer {
    constructor() {
        this.activityLog = new Map();
    }
    async getActivityScore(opportunity) {
        const key = `${opportunity.buyPool.toString()}-${opportunity.sellPool.toString()}`;
        const activities = this.activityLog.get(key) || [];
        const recentActivities = activities.filter(a => Date.now() - a.timestamp < 300000);
        if (recentActivities.length === 0)
            return 0;
        const totalIntensity = recentActivities.reduce((sum, a) => sum + a.intensity, 0);
        return Math.min(1, totalIntensity / 100);
    }
    async getRecentActivity(buyPool, sellPool) {
        const key = `${buyPool.toString()}-${sellPool.toString()}`;
        const activities = this.activityLog.get(key) || [];
        return activities.filter(a => Date.now() - a.timestamp < 600000);
    }
    recordActivity(buyPool, sellPool, intensity) {
        const key = `${buyPool.toString()}-${sellPool.toString()}`;
        const activities = this.activityLog.get(key) || [];
        activities.push({ timestamp: Date.now(), intensity });
        const maxActivities = 1000;
        if (activities.length > maxActivities) {
            activities.splice(0, activities.length - maxActivities);
        }
        this.activityLog.set(key, activities);
    }
}
class ProfitabilityEngine {
    constructor(connection) {
        this.MIN_VOLUME_THRESHOLD = 100000;
        this.MAX_POSITION_SIZE = 0.02;
        this.SAFETY_MARGIN = 1.15;
        this.predictor = new FlashLoanProfitabilityPredictor(connection);
    }
    async evaluateOpportunity(opportunity) {
        try {
            const metrics = await this.predictor.analyzeProfitability(opportunity);
            const volumeCheck = await this.checkVolumeRequirements(opportunity);
            const positionSizeCheck = this.checkPositionSize(opportunity, metrics);
            const safetyCheck = this.performSafetyChecks(metrics);
            const shouldExecute = this.predictor.isProfitable(metrics) &&
                volumeCheck &&
                positionSizeCheck &&
                safetyCheck;
            const reasoning = this.generateReasoning(metrics, volumeCheck, positionSizeCheck, safetyCheck);
            return {
                shouldExecute,
                metrics,
                executionParams: shouldExecute
                    ? this.predictor.getOptimalExecutionParams(opportunity, metrics)
                    : null,
                confidence: metrics.successProbability,
                reasoning
            };
        }
        catch (error) {
            return {
                shouldExecute: false,
                metrics: this.getEmptyMetrics(),
                executionParams: null,
                confidence: 0,
                reasoning: `Error evaluating opportunity: ${error.message || String(error)}`
            };
        }
    }
    async checkVolumeRequirements(opportunity) {
        const volume = await this.predictor['getRecentTradeVolume'](opportunity.tokenA, opportunity.tokenB);
        return volume >= this.MIN_VOLUME_THRESHOLD;
    }
    checkPositionSize(opportunity, metrics) {
        const positionSize = metrics.optimalLoanAmount.toNumber();
        const maxAllowed = this.MIN_VOLUME_THRESHOLD * this.MAX_POSITION_SIZE;
        return positionSize <= maxAllowed;
    }
    performSafetyChecks(metrics) {
        return (metrics.roi >= this.predictor['MIN_PROFIT_BASIS_POINTS'] / 100 * this.SAFETY_MARGIN &&
            metrics.riskScore >= 0.75 &&
            metrics.maxDrawdown <= 0.05 &&
            metrics.sharpeRatio >= 1.5);
    }
    generateReasoning(metrics, volumeCheck, positionSizeCheck, safetyCheck) {
        const reasons = [];
        if (!volumeCheck)
            reasons.push('Insufficient trading volume');
        if (!positionSizeCheck)
            reasons.push('Position size too large');
        if (!safetyCheck)
            reasons.push('Failed safety checks');
        if (metrics.roi < 0.15)
            reasons.push(`ROI too low: ${metrics.roi.toFixed(2)}%`);
        if (metrics.successProbability < 0.85) {
            reasons.push(`Success probability too low: ${(metrics.successProbability * 100).toFixed(1)}%`);
        }
        if (metrics.riskScore < 0.7) {
            reasons.push(`Risk score too low: ${metrics.riskScore.toFixed(2)}`);
        }
        return reasons.length > 0
            ? reasons.join('; ')
            : `Profitable opportunity: ROI ${metrics.roi.toFixed(2)}%, ` +
                `Success ${(metrics.successProbability * 100).toFixed(1)}%`;
    }
    getEmptyMetrics() {
        return {
            grossProfit: new bn_js_1.default(0),
            netProfit: new bn_js_1.default(0),
            roi: 0,
            riskScore: 0,
            successProbability: 0,
            optimalLoanAmount: new bn_js_1.default(0),
            breakEvenPrice: 0,
            maxDrawdown: 1,
            sharpeRatio: 0
        };
    }
    async recordExecution(opportunity, result) {
        const conditions = await this.predictor['analyzeMarketConditions'](opportunity);
        await this.predictor.updateHistoricalData(opportunity, {
            ...result,
            conditions
        });
        if (result.success) {
            this.predictor['competitionAnalyzer'].recordActivity(opportunity.buyPool, opportunity.sellPool, result.profit > 0 ? 1 : 0.5);
        }
    }
    getPredictor() {
        return this.predictor;
    }
}
exports.ProfitabilityEngine = ProfitabilityEngine;
const createProfitabilityPredictor = (connection) => {
    return new ProfitabilityEngine(connection);
};
exports.createProfitabilityPredictor = createProfitabilityPredictor;
const calculateRequiredProfit = (loanAmount, gasEstimate, minProfitBasisPoints = 15) => {
    const gasCost = new bn_js_1.default(gasEstimate);
    const minProfit = loanAmount.mul(new bn_js_1.default(minProfitBasisPoints)).div(new bn_js_1.default(10000));
    return gasCost.add(minProfit);
};
exports.calculateRequiredProfit = calculateRequiredProfit;
const estimateOptimalTiming = (volatility, competitionScore, marketConditions) => {
    const baseDelay = 100;
    const volatilityMultiplier = Math.max(0.5, 1 - volatility * 2);
    const competitionMultiplier = Math.max(0.3, 1 - competitionScore * 2);
    const spreadMultiplier = Math.max(0.5, 1 - marketConditions.bidAskSpread * 10);
    const executeIn = Math.floor(baseDelay * volatilityMultiplier * competitionMultiplier * spreadMultiplier);
    const urgency = executeIn < 50 ? 'high' :
        executeIn < 200 ? 'medium' : 'low';
    return { executeIn, urgency };
};
exports.estimateOptimalTiming = estimateOptimalTiming;
const validateFlashLoanParams = (params) => {
    const errors = [];
    if (params.loanAmount.lte(new bn_js_1.default(0))) {
        errors.push('Loan amount must be positive');
    }
    if (params.repaymentAmount.lte(params.loanAmount)) {
        errors.push('Repayment amount must exceed loan amount');
    }
    if (params.gasLimit < 100000 || params.gasLimit > 1000000) {
        errors.push('Gas limit out of range');
    }
    if (params.priority < 0 || params.priority > 10000) {
        errors.push('Priority fee out of range');
    }
    return {
        valid: errors.length === 0,
        errors
    };
};
exports.validateFlashLoanParams = validateFlashLoanParams;
exports.default = FlashLoanProfitabilityPredictor;
//# sourceMappingURL=FlashLoanProfitabilityPredictor.js.map