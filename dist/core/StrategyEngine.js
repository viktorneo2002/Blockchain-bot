"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createOptimizedEngine = exports.StrategyEngineBuilder = exports.StrategyEngine = exports.SignalType = exports.StrategyType = void 0;
const web3_js_1 = require("@solana/web3.js");
const spl_token_1 = require("@solana/spl-token");
const bn_js_1 = __importDefault(require("bn.js"));
const events_1 = require("events");
var StrategyType;
(function (StrategyType) {
    StrategyType["ARBITRAGE"] = "ARBITRAGE";
    StrategyType["MOMENTUM"] = "MOMENTUM";
    StrategyType["MEAN_REVERSION"] = "MEAN_REVERSION";
    StrategyType["LIQUIDITY_PROVISION"] = "LIQUIDITY_PROVISION";
    StrategyType["STATISTICAL_ARB"] = "STATISTICAL_ARB";
    StrategyType["MARKET_MAKING"] = "MARKET_MAKING";
})(StrategyType || (exports.StrategyType = StrategyType = {}));
var SignalType;
(function (SignalType) {
    SignalType["STRONG_BUY"] = "STRONG_BUY";
    SignalType["BUY"] = "BUY";
    SignalType["NEUTRAL"] = "NEUTRAL";
    SignalType["SELL"] = "SELL";
    SignalType["STRONG_SELL"] = "STRONG_SELL";
})(SignalType || (exports.SignalType = SignalType = {}));
class StrategyEngine extends events_1.EventEmitter {
    constructor(connection, config) {
        super();
        this.connection = connection;
        this.config = config;
        this.positions = new Map();
        this.marketCache = new Map();
        this.isRunning = false;
        this.performanceMetrics = new PerformanceMetrics();
        this.strategies = this.initializeStrategies();
        this.riskManager = new RiskManager(config);
        this.signalAggregator = new SignalAggregator();
    }
    initializeStrategies() {
        const strategies = new Map();
        strategies.set(StrategyType.ARBITRAGE, new ArbitrageStrategy(this.config));
        strategies.set(StrategyType.MOMENTUM, new MomentumStrategy(this.config));
        strategies.set(StrategyType.MEAN_REVERSION, new MeanReversionStrategy(this.config));
        strategies.set(StrategyType.LIQUIDITY_PROVISION, new LiquidityStrategy(this.config));
        strategies.set(StrategyType.STATISTICAL_ARB, new StatisticalArbStrategy(this.config));
        strategies.set(StrategyType.MARKET_MAKING, new MarketMakingStrategy(this.config));
        return strategies;
    }
    async start() {
        if (this.isRunning)
            return;
        this.isRunning = true;
        this.emit('engine:started');
        await this.runStrategies();
    }
    async stop() {
        this.isRunning = false;
        await this.closeAllPositions();
        this.emit('engine:stopped');
    }
    async runStrategies() {
        while (this.isRunning) {
            try {
                const marketData = await this.fetchMarketData();
                const signals = await this.generateSignals(marketData);
                const validatedSignals = await this.validateSignals(signals);
                await this.executeSignals(validatedSignals);
                await this.updatePositions();
                await this.performRiskCheck();
                await this.sleep(100);
            }
            catch (error) {
                this.emit('error', error);
                await this.handleError(error);
            }
        }
    }
    async fetchMarketData() {
        const data = new Map();
        const tokens = await this.getMonitoredTokens();
        await Promise.all(tokens.map(async (token) => {
            const marketData = await this.getTokenMarketData(token);
            data.set(token.toString(), marketData);
            this.marketCache.set(token.toString(), marketData);
        }));
        return data;
    }
    async generateSignals(marketData) {
        const allSignals = [];
        for (const [strategyType, strategy] of this.strategies) {
            const signals = await strategy.analyze(marketData, this.positions);
            allSignals.push(...signals);
        }
        const aggregatedSignals = this.signalAggregator.aggregate(allSignals);
        const prioritizedSignals = this.prioritizeSignals(aggregatedSignals);
        return prioritizedSignals;
    }
    async validateSignals(signals) {
        const validated = [];
        for (const signal of signals) {
            if (await this.isSignalValid(signal)) {
                validated.push(signal);
            }
        }
        return validated.slice(0, 10);
    }
    async isSignalValid(signal) {
        if (signal.confidence < 0.7)
            return false;
        const marketData = this.marketCache.get(signal.token.toString());
        if (!marketData)
            return false;
        if (marketData.liquidity.lt(this.config.minLiquidityThreshold))
            return false;
        const riskCheck = this.riskManager.evaluateSignal(signal, this.positions);
        if (!riskCheck.approved)
            return false;
        if (this.config.mevProtection && await this.detectMEV(signal))
            return false;
        if (this.config.frontrunDetection && await this.detectFrontrun(signal))
            return false;
        return true;
    }
    async executeSignals(signals) {
        for (const signal of signals) {
            try {
                await this.executeSignal(signal);
            }
            catch (error) {
                this.emit('execution:error', { signal, error });
            }
        }
    }
    async executeSignal(signal) {
        const transaction = new web3_js_1.Transaction();
        const instructions = await this.buildInstructions(signal);
        if (instructions.length === 0)
            return;
        transaction.add(...instructions);
        const txSize = transaction.serialize({ requireAllSignatures: false }).length;
        if (txSize > 1232) {
            throw new Error('Transaction too large');
        }
        const gasPrice = await this.estimateGasPrice();
        if (gasPrice.gt(this.config.maxGasPrice)) {
            this.emit('gas:high', { price: gasPrice });
            return;
        }
        await this.sendOptimizedTransaction(transaction, signal);
    }
    async buildInstructions(signal) {
        const instructions = [];
        if (signal.action === 'BUY') {
            instructions.push(...await this.buildBuyInstructions(signal));
        }
        else if (signal.action === 'SELL') {
            instructions.push(...await this.buildSellInstructions(signal));
        }
        return instructions;
    }
    async sendOptimizedTransaction(transaction, signal) {
        const recentBlockhash = await this.connection.getRecentBlockhash();
        transaction.recentBlockhash = recentBlockhash.blockhash;
        const simulation = await this.connection.simulateTransaction(transaction);
        if (simulation.value.err) {
            throw new Error(`Simulation failed: ${JSON.stringify(simulation.value.err)}`);
        }
        const signature = await this.connection.sendRawTransaction(transaction.serialize(), {
            skipPreflight: true,
            maxRetries: 3,
            preflightCommitment: 'processed'
        });
        await this.connection.confirmTransaction(signature, 'confirmed');
        this.updatePosition(signal, signature);
        this.performanceMetrics.recordTrade(signal, signature);
        this.emit('trade:executed', { signal, signature });
    }
    prioritizeSignals(signals) {
        return signals.sort((a, b) => {
            const scoreA = this.calculateSignalScore(a);
            const scoreB = this.calculateSignalScore(b);
            return scoreB - scoreA;
        });
    }
    calculateSignalScore(signal) {
        const marketData = this.marketCache.get(signal.token.toString());
        if (!marketData)
            return 0;
        let score = signal.confidence * signal.strength;
        score *= (1 + marketData.momentum * 0.1);
        score *= (1 + Math.min(marketData.volume24h.toNumber() / 1000000, 1) * 0.2);
        score *= (1 - marketData.spread * 10);
        const expectedProfit = (signal.targetPrice - marketData.price) / marketData.price;
        score *= (1 + expectedProfit * 10);
        return score;
    }
    async updatePositions() {
        for (const [tokenStr, position] of this.positions) {
            const marketData = this.marketCache.get(tokenStr);
            if (!marketData)
                continue;
            position.unrealizedPnl = ((marketData.price - position.entryPrice) / position.entryPrice) * 100;
            if (await this.shouldClosePosition(position, marketData)) {
                await this.closePosition(position);
            }
        }
    }
    async shouldClosePosition(position, marketData) {
        if (position.unrealizedPnl > 5)
            return true;
        if (position.unrealizedPnl < -2)
            return true;
        const holdTime = Date.now() - position.timestamp;
        if (holdTime > 3600000 && position.unrealizedPnl > 0)
            return true;
        const strategy = this.strategies.get(position.strategy);
        if (strategy) {
            return await strategy.shouldExit(position, marketData);
        }
        return false;
    }
    async performRiskCheck() {
        const totalExposure = this.calculateTotalExposure();
        const riskMetrics = this.riskManager.calculateMetrics(this.positions, this.marketCache);
        if (riskMetrics.var > this.config.riskLimit) {
            await this.reduceExposure();
        }
        if (riskMetrics.sharpeRatio < 1.5) {
            this.adjustStrategyParameters();
        }
        this.emit('risk:update', riskMetrics);
    }
    calculateTotalExposure() {
        let total = 0;
        for (const position of this.positions.values()) {
            const marketData = this.marketCache.get(position.token.toString());
            if (marketData) {
                total += position.amount.toNumber() * marketData.price;
            }
        }
        return total;
    }
    adjustStrategyParameters() {
        if (this.config.dynamicFeeAdjustment) {
            const performance = this.performanceMetrics.getRecentPerformance();
            if (performance.winRate < 0.6) {
                this.config.minProfitBps = Math.min(this.config.minProfitBps * 1.1, 100);
            }
            else if (performance.winRate > 0.8) {
                this.config.minProfitBps = Math.max(this.config.minProfitBps * 0.9, 10);
            }
        }
    }
    async detectMEV(signal) {
        const recentBlocks = await this.connection.getRecentPerformanceSamples(10);
        const avgBlockTime = recentBlocks.reduce((acc, b) => acc + b.samplePeriodSecs, 0) / recentBlocks.length;
        if (avgBlockTime < 0.4) {
            const mempool = await this.scanMempool(signal.token);
            return mempool.suspiciousActivity;
        }
        return false;
    }
    async detectFrontrun(signal) {
        const pendingTxs = await this.getPendingTransactions(signal.token);
        const similarTxs = pendingTxs.filter(tx => this.isSimilarTransaction(tx, signal));
        return similarTxs.length > 2;
    }
    async scanMempool(token) {
        return { suspiciousActivity: false };
    }
    async getPendingTransactions(token) {
        return [];
    }
    isSimilarTransaction(tx, signal) {
        if (!tx || !tx.message)
            return false;
        const instructions = tx.message.instructions;
        for (const instruction of instructions) {
            if (instruction.programId.equals(spl_token_1.TOKEN_PROGRAM_ID)) {
                const decoded = this.decodeTokenInstruction(instruction);
                if (decoded && decoded.mint.equals(signal.token)) {
                    return true;
                }
            }
        }
        return false;
    }
    decodeTokenInstruction(instruction) {
        try {
            return (0, spl_token_1.decodeInstruction)(instruction);
        }
        catch {
            return null;
        }
    }
    async estimateGasPrice() {
        const recentFees = await this.connection.getRecentBlockhash();
        const baseFee = new bn_js_1.default(recentFees.feeCalculator.lamportsPerSignature);
        const priorityFee = this.calculatePriorityFee();
        return baseFee.add(priorityFee);
    }
    calculatePriorityFee() {
        const baseUnits = 5000;
        const urgencyMultiplier = this.isHighPriorityTime() ? 3 : 1;
        return new bn_js_1.default(baseUnits * urgencyMultiplier);
    }
    isHighPriorityTime() {
        const hour = new Date().getUTCHours();
        return (hour >= 13 && hour <= 17) || (hour >= 1 && hour <= 4);
    }
    async getMonitoredTokens() {
        const activeTokens = [];
        for (const position of this.positions.values()) {
            activeTokens.push(position.token);
        }
        return activeTokens;
    }
    async getTokenMarketData(token) {
        const price = await this.fetchTokenPrice(token);
        const volume = await this.fetchTokenVolume(token);
        const liquidity = await this.fetchTokenLiquidity(token);
        const orderbook = await this.fetchOrderbook(token);
        const volatility = this.calculateVolatility(token);
        const momentum = this.calculateMomentum(token);
        const vwap = this.calculateVWAP(token);
        return {
            price,
            volume24h: volume,
            liquidity,
            volatility,
            spread: this.calculateSpread(orderbook),
            depth: orderbook,
            momentum,
            vwap
        };
    }
    async fetchTokenPrice(token) {
        return 0;
    }
    async fetchTokenVolume(token) {
        return new bn_js_1.default(0);
    }
    async fetchTokenLiquidity(token) {
        return new bn_js_1.default(0);
    }
    async fetchOrderbook(token) {
        return { bids: new bn_js_1.default(0), asks: new bn_js_1.default(0) };
    }
    calculateVolatility(token) {
        return 0;
    }
    calculateMomentum(token) {
        return 0;
    }
    calculateVWAP(token) {
        return 0;
    }
    calculateSpread(orderbook) {
        return 0;
    }
    async buildBuyInstructions(signal) {
        return [];
    }
    async buildSellInstructions(signal) {
        return [];
    }
    updatePosition(signal, signature) {
        if (signal.action === 'BUY') {
            const position = {
                token: signal.token,
                amount: new bn_js_1.default(0),
                entryPrice: signal.targetPrice,
                timestamp: Date.now(),
                unrealizedPnl: 0,
                strategy: StrategyType.MOMENTUM
            };
            this.positions.set(signal.token.toString(), position);
        }
        else if (signal.action === 'SELL') {
            this.positions.delete(signal.token.toString());
        }
    }
    async closePosition(position) {
        const signal = {
            type: SignalType.SELL,
            strength: 1,
            token: position.token,
            action: 'SELL',
            targetPrice: 0,
            stopLoss: 0,
            takeProfit: 0,
            confidence: 1,
            timeframe: 0
        };
        await this.executeSignal(signal);
    }
    async closeAllPositions() {
        const promises = Array.from(this.positions.values()).map(position => this.closePosition(position));
        await Promise.all(promises);
    }
    async reduceExposure() {
        const sortedPositions = Array.from(this.positions.values())
            .sort((a, b) => a.unrealizedPnl - b.unrealizedPnl);
        const toClose = Math.ceil(sortedPositions.length * 0.3);
        for (let i = 0; i < toClose; i++) {
            await this.closePosition(sortedPositions[i]);
        }
    }
    async handleError(error) {
        console.error('Strategy engine error:', error);
        if (error.message?.includes('insufficient funds')) {
            await this.reduceExposure();
        }
        else if (error.message?.includes('rate limit')) {
            await this.sleep(5000);
        }
        else if (error.message?.includes('network')) {
            await this.sleep(1000);
        }
    }
    async sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}
exports.StrategyEngine = StrategyEngine;
class ArbitrageStrategy {
    constructor(config) {
        this.config = config;
    }
    async analyze(marketData, positions) {
        const signals = [];
        for (const [token, data] of marketData) {
            const opportunity = this.findArbitrageOpportunity(token, data);
            if (opportunity) {
                signals.push(this.createSignal(token, opportunity));
            }
        }
        return signals;
    }
    async shouldExit(position, marketData) {
        return position.unrealizedPnl > 0.5 || position.unrealizedPnl < -0.3;
    }
    findArbitrageOpportunity(token, data) {
        if (data.spread > 0.001) {
            return { type: 'spread', expectedProfit: data.spread };
        }
        return null;
    }
    createSignal(token, opportunity) {
        return {
            type: SignalType.BUY,
            strength: opportunity.expectedProfit * 100,
            token: new web3_js_1.PublicKey(token),
            action: 'BUY',
            targetPrice: 0,
            stopLoss: 0,
            takeProfit: 0,
            confidence: Math.min(opportunity.expectedProfit * 200, 1),
            timeframe: 60
        };
    }
}
class MomentumStrategy {
    constructor(config) {
        this.config = config;
    }
    async analyze(marketData, positions) {
        const signals = [];
        for (const [token, data] of marketData) {
            if (data.momentum > 0.5 && data.volume24h.gt(new bn_js_1.default(1000000))) {
                signals.push(this.createBuySignal(token, data));
            }
            else if (data.momentum < -0.5) {
                signals.push(this.createSellSignal(token, data));
            }
        }
        return signals;
    }
    async shouldExit(position, marketData) {
        return marketData.momentum < 0 || position.unrealizedPnl > 2;
    }
    createBuySignal(token, data) {
        return {
            type: SignalType.BUY,
            strength: data.momentum,
            token: new web3_js_1.PublicKey(token),
            action: 'BUY',
            targetPrice: data.price * 1.02,
            stopLoss: data.price * 0.98,
            takeProfit: data.price * 1.05,
            confidence: Math.min(data.momentum, 0.9),
            timeframe: 300
        };
    }
    createSellSignal(token, data) {
        return {
            type: SignalType.SELL,
            strength: Math.abs(data.momentum),
            token: new web3_js_1.PublicKey(token),
            action: 'SELL',
            targetPrice: data.price * 0.98,
            stopLoss: data.price * 1.02,
            takeProfit: data.price * 0.95,
            confidence: Math.min(Math.abs(data.momentum), 0.9),
            timeframe: 300
        };
    }
}
class MeanReversionStrategy {
    constructor(config) {
        this.config = config;
    }
    async analyze(marketData, positions) {
        const signals = [];
        for (const [token, data] of marketData) {
            const deviation = (data.price - data.vwap) / data.vwap;
            if (deviation < -0.02 && data.volatility < 0.3) {
                signals.push(this.createBuySignal(token, data, deviation));
            }
            else if (deviation > 0.02) {
                signals.push(this.createSellSignal(token, data, deviation));
            }
        }
        return signals;
    }
    async shouldExit(position, marketData) {
        const deviation = Math.abs((marketData.price - marketData.vwap) / marketData.vwap);
        return deviation < 0.005;
    }
    createBuySignal(token, data, deviation) {
        return {
            type: SignalType.BUY,
            strength: Math.abs(deviation) * 10,
            token: new web3_js_1.PublicKey(token),
            action: 'BUY',
            targetPrice: data.vwap,
            stopLoss: data.price * 0.97,
            takeProfit: data.vwap * 1.01,
            confidence: 0.8,
            timeframe: 600
        };
    }
    createSellSignal(token, data, deviation) {
        return {
            type: SignalType.SELL,
            strength: Math.abs(deviation) * 10,
            token: new web3_js_1.PublicKey(token),
            action: 'SELL',
            targetPrice: data.vwap,
            stopLoss: data.price * 1.03,
            takeProfit: data.vwap * 0.99,
            confidence: 0.8,
            timeframe: 600
        };
    }
}
class LiquidityStrategy {
    constructor(config) {
        this.config = config;
    }
    async analyze(marketData, positions) {
        return [];
    }
    async shouldExit(position, marketData) {
        return false;
    }
}
class StatisticalArbStrategy {
    constructor(config) {
        this.config = config;
    }
    async analyze(marketData, positions) {
        return [];
    }
    async shouldExit(position, marketData) {
        return false;
    }
}
class MarketMakingStrategy {
    constructor(config) {
        this.config = config;
    }
    async analyze(marketData, positions) {
        return [];
    }
    async shouldExit(position, marketData) {
        return false;
    }
}
class RiskManager {
    constructor(config) {
        this.config = config;
    }
    evaluateSignal(signal, positions) {
        const positionCount = positions.size;
        if (positionCount >= 10) {
            return { approved: false, reason: 'Max positions reached' };
        }
        return { approved: true };
    }
    calculateMetrics(positions, marketCache) {
        return {
            var: 0,
            sharpeRatio: 2.0,
            maxDrawdown: 0,
            correlation: 0
        };
    }
}
class SignalAggregator {
    aggregate(signals) {
        const grouped = new Map();
        for (const signal of signals) {
            const key = signal.token.toString();
            if (!grouped.has(key)) {
                grouped.set(key, []);
            }
            grouped.get(key).push(signal);
        }
        const aggregated = [];
        for (const [token, tokenSignals] of grouped) {
            if (tokenSignals.length >= 2) {
                const combined = this.combineSignals(tokenSignals);
                aggregated.push(combined);
            }
        }
        return aggregated;
    }
    combineSignals(signals) {
        const avgStrength = signals.reduce((acc, s) => acc + s.strength, 0) / signals.length;
        const avgConfidence = signals.reduce((acc, s) => acc + s.confidence, 0) / signals.length;
        return {
            ...signals[0],
            strength: avgStrength,
            confidence: avgConfidence
        };
    }
}
class PerformanceMetrics {
    constructor() {
        this.trades = [];
    }
    recordTrade(signal, signature) {
        this.trades.push({
            signal,
            signature,
            timestamp: Date.now(),
            result: 'pending'
        });
    }
    getRecentPerformance() {
        const recentTrades = this.trades.filter(t => Date.now() - t.timestamp < 24 * 60 * 60 * 1000);
        const wins = recentTrades.filter(t => t.result === 'win').length;
        const winRate = recentTrades.length > 0 ? wins / recentTrades.length : 0;
        const profits = recentTrades
            .filter(t => t.profit !== undefined)
            .map(t => t.profit);
        const avgProfit = profits.length > 0
            ? profits.reduce((a, b) => a + b, 0) / profits.length
            : 0;
        return {
            winRate,
            avgProfit,
            totalTrades: recentTrades.length
        };
    }
    updateTradeResult(signature, profit) {
        const trade = this.trades.find(t => t.signature === signature);
        if (trade) {
            trade.result = profit > 0 ? 'win' : 'loss';
            trade.profit = profit;
        }
    }
    getDailyStats() {
        const dayStart = Date.now() - 24 * 60 * 60 * 1000;
        const dayTrades = this.trades.filter(t => t.timestamp > dayStart);
        const volume = dayTrades.reduce((sum, t) => sum + (t.volume || 0), 0);
        const pnl = dayTrades.reduce((sum, t) => sum + (t.profit || 0), 0);
        const returns = dayTrades
            .filter(t => t.profit !== undefined)
            .map(t => t.profit);
        const sharpe = this.calculateSharpe(returns);
        return {
            volume,
            trades: dayTrades.length,
            pnl,
            sharpe
        };
    }
    calculateSharpe(returns) {
        if (returns.length < 2)
            return 0;
        const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
        const variance = returns.reduce((sum, r) => sum + Math.pow(r - mean, 2), 0) / returns.length;
        const stdDev = Math.sqrt(variance);
        return stdDev > 0 ? (mean / stdDev) * Math.sqrt(365) : 0;
    }
}
class StrategyEngineBuilder {
    constructor() {
        this.config = {};
    }
    withMinProfit(bps) {
        this.config.minProfitBps = bps;
        return this;
    }
    withMaxPosition(size) {
        this.config.maxPositionSize = size;
        return this;
    }
    withSlippage(bps) {
        this.config.maxSlippageBps = bps;
        return this;
    }
    withLiquidity(threshold) {
        this.config.minLiquidityThreshold = threshold;
        return this;
    }
    withGasLimit(maxGas) {
        this.config.maxGasPrice = maxGas;
        return this;
    }
    withRiskLimit(limit) {
        this.config.riskLimit = limit;
        return this;
    }
    withRebalance(threshold) {
        this.config.rebalanceThreshold = threshold;
        return this;
    }
    enableDynamicFees() {
        this.config.dynamicFeeAdjustment = true;
        return this;
    }
    enableMEVProtection() {
        this.config.mevProtection = true;
        return this;
    }
    enableFrontrunDetection() {
        this.config.frontrunDetection = true;
        return this;
    }
    enableMultiPool() {
        this.config.multiPoolArbitrage = true;
        return this;
    }
    enableVolumeWeighting() {
        this.config.volumeWeightedEntry = true;
        return this;
    }
    build(connection) {
        const defaultConfig = {
            minProfitBps: 30,
            maxPositionSize: new bn_js_1.default(1000000),
            maxSlippageBps: 50,
            minLiquidityThreshold: new bn_js_1.default(100000),
            maxGasPrice: new bn_js_1.default(1000000),
            riskLimit: 0.02,
            rebalanceThreshold: 0.1,
            dynamicFeeAdjustment: true,
            mevProtection: true,
            frontrunDetection: true,
            multiPoolArbitrage: true,
            volumeWeightedEntry: true
        };
        const finalConfig = { ...defaultConfig, ...this.config };
        return new StrategyEngine(connection, finalConfig);
    }
}
exports.StrategyEngineBuilder = StrategyEngineBuilder;
const createOptimizedEngine = (connection) => {
    return new StrategyEngineBuilder()
        .withMinProfit(25)
        .withMaxPosition(new bn_js_1.default(5000000))
        .withSlippage(30)
        .withLiquidity(new bn_js_1.default(500000))
        .withGasLimit(new bn_js_1.default(2000000))
        .withRiskLimit(0.015)
        .withRebalance(0.08)
        .enableDynamicFees()
        .enableMEVProtection()
        .enableFrontrunDetection()
        .enableMultiPool()
        .enableVolumeWeighting()
        .build(connection);
};
exports.createOptimizedEngine = createOptimizedEngine;
exports.default = StrategyEngine;
//# sourceMappingURL=StrategyEngine.js.map