"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.PendingTxMonitor = void 0;
const web3_js_1 = require("@solana/web3.js");
const events_1 = require("events");
const ws_1 = __importDefault(require("ws"));
const anchor_1 = require("@project-serum/anchor");
class PendingTxMonitor extends events_1.EventEmitter {
    constructor(config) {
        super();
        this.ws = null;
        this.isRunning = false;
        this.reconnectAttempts = 0;
        this.subscriptionId = null;
        this.seenSignatures = new Set();
        this.poolCache = new Map();
        this.pendingTxQueue = [];
        this.processingInterval = null;
        this.metricsInterval = null;
        this.lastBlockTime = Date.now();
        // Performance metrics
        this.metrics = {
            txProcessed: 0,
            opportunitiesFound: 0,
            avgLatency: 0,
            missedOpportunities: 0,
            errors: 0,
            uptime: Date.now()
        };
        // Known DEX program IDs
        this.DEX_PROGRAMS = {
            RAYDIUM_V4: '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            ORCA_WHIRLPOOL: 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
            ORCA_V2: '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
            SERUM_V3: '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'
        };
        this.config = {
            ...config,
            targetPrograms: config.targetPrograms.length > 0 ? config.targetPrograms : Object.values(this.DEX_PROGRAMS)
        };
        this.connection = new web3_js_1.Connection(config.rpcEndpoint, {
            commitment: config.commitment,
            wsEndpoint: config.wsEndpoint
        });
    }
    async start() {
        if (this.isRunning) {
            throw new Error('Monitor already running');
        }
        this.isRunning = true;
        this.emit('started');
        await this.initializePoolCache();
        await this.connectWebSocket();
        this.startProcessingLoop();
        this.startMetricsReporting();
    }
    async stop() {
        this.isRunning = false;
        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
        if (this.processingInterval) {
            clearInterval(this.processingInterval);
            this.processingInterval = null;
        }
        if (this.metricsInterval) {
            clearInterval(this.metricsInterval);
            this.metricsInterval = null;
        }
        this.emit('stopped');
    }
    async connectWebSocket() {
        try {
            this.ws = new ws_1.default(this.config.wsEndpoint);
            this.ws.on('open', () => {
                this.reconnectAttempts = 0;
                this.subscribeToPrograms();
                this.emit('connected');
            });
            this.ws.on('message', (data) => {
                this.handleWebSocketMessage(data);
            });
            this.ws.on('error', (error) => {
                this.metrics.errors++;
                this.emit('error', error);
            });
            this.ws.on('close', () => {
                this.handleDisconnection();
            });
        }
        catch (error) {
            this.emit('error', error);
            await this.handleReconnection();
        }
    }
    async subscribeToPrograms() {
        if (!this.ws || this.ws.readyState !== ws_1.default.OPEN)
            return;
        const subscribeMessage = {
            jsonrpc: '2.0',
            id: 1,
            method: 'logsSubscribe',
            params: [
                {
                    mentions: this.config.targetPrograms
                },
                {
                    commitment: this.config.commitment
                }
            ]
        };
        this.ws.send(JSON.stringify(subscribeMessage));
        // Subscribe to slot updates for timing
        const slotSubscribe = {
            jsonrpc: '2.0',
            id: 2,
            method: 'slotSubscribe',
            params: []
        };
        this.ws.send(JSON.stringify(slotSubscribe));
    }
    async handleWebSocketMessage(data) {
        try {
            const message = JSON.parse(data.toString());
            if (message.method === 'logsNotification') {
                await this.processLogNotification(message.params);
            }
            else if (message.method === 'slotNotification') {
                this.updateSlotInfo(message.params);
            }
        }
        catch (error) {
            this.metrics.errors++;
            this.emit('error', error);
        }
    }
    async processLogNotification(params) {
        const startTime = Date.now();
        try {
            const { result, subscription } = params;
            const { signature, logs, err } = result.value;
            if (err || this.seenSignatures.has(signature))
                return;
            this.seenSignatures.add(signature);
            // Cleanup old signatures periodically
            if (this.seenSignatures.size > 10000) {
                const toDelete = Array.from(this.seenSignatures).slice(0, 5000);
                toDelete.forEach(sig => this.seenSignatures.delete(sig));
            }
            const transaction = await this.fetchTransaction(signature);
            if (!transaction)
                return;
            const pendingTx = await this.parseTransaction(transaction, signature);
            if (!pendingTx)
                return;
            this.pendingTxQueue.push(pendingTx);
            this.metrics.txProcessed++;
            const latency = Date.now() - startTime;
            this.metrics.avgLatency = (this.metrics.avgLatency * 0.9) + (latency * 0.1);
        }
        catch (error) {
            this.metrics.errors++;
            this.emit('error', error);
        }
    }
    async fetchTransaction(signature) {
        try {
            const tx = await this.connection.getParsedTransaction(signature, {
                maxSupportedTransactionVersion: 0
            });
            return tx;
        }
        catch (error) {
            return null;
        }
    }
    async parseTransaction(tx, signature) {
        try {
            const message = tx.transaction.message;
            const instruction = message.instructions[0];
            if (!instruction)
                return null;
            // Type guard for PartiallyDecodedInstruction
            if ('programIdIndex' in instruction) {
                const programIdIndex = instruction.programIdIndex;
                if (programIdIndex === undefined)
                    return null;
                if (typeof programIdIndex !== "number" || programIdIndex >= message.accountKeys.length)
                    return null;
                const programId = message.accountKeys[programIdIndex].pubkey;
                if (!this.config.targetPrograms.includes(programId.toString())) {
                    return null;
                }
                const accounts = message.accountKeys.map(key => key.pubkey);
                return {
                    signature,
                    transaction: tx.transaction,
                    slot: tx.slot,
                    timestamp: Date.now(),
                    programId: new web3_js_1.PublicKey(programId),
                    accounts,
                    data: 'data' in instruction ? Buffer.from(instruction.data || '', 'base64') : Buffer.alloc(0)
                };
            }
            return null;
        }
        catch (error) {
            return null;
        }
    }
    startProcessingLoop() {
        setInterval(() => {
            this.processQueuedTransactions();
        }, 10);
    }
    async processQueuedTransactions() {
        while (this.pendingTxQueue.length > 0) {
            const pendingTx = this.pendingTxQueue.shift();
            if (!pendingTx)
                continue;
            const age = Date.now() - pendingTx.timestamp;
            if (age > this.config.maxLatencyMs) {
                this.metrics.missedOpportunities++;
                continue;
            }
            await this.analyzeTradingOpportunity(pendingTx);
        }
    }
    async analyzeTradingOpportunity(pendingTx) {
        try {
            const programId = pendingTx.programId.toString();
            switch (programId) {
                case this.DEX_PROGRAMS.RAYDIUM_V4:
                    await this.analyzeRaydiumTx(pendingTx);
                    break;
                case this.DEX_PROGRAMS.ORCA_WHIRLPOOL:
                    await this.analyzeOrcaWhirlpoolTx(pendingTx);
                    break;
                case this.DEX_PROGRAMS.ORCA_V2:
                    await this.analyzeOrcaV2Tx(pendingTx);
                    break;
                case this.DEX_PROGRAMS.SERUM_V3:
                    await this.analyzeSerumTx(pendingTx);
                    break;
            }
        }
        catch (error) {
            this.emit('error', error);
        }
    }
    async analyzeRaydiumTx(pendingTx) {
        const data = pendingTx.data;
        if (data.length < 1)
            return;
        const instructionType = data[0];
        // Raydium swap instruction = 9
        if (instructionType !== 9)
            return;
        const poolAddress = pendingTx.accounts[1];
        const poolState = await this.getPoolState(poolAddress);
        if (!poolState)
            return;
        const amountIn = this.decodeAmount(data.slice(1, 9));
        const minAmountOut = this.decodeAmount(data.slice(9, 17));
        const priceImpact = this.calculatePriceImpact(amountIn, poolState.reserveA, poolState.reserveB, poolState.fee);
        if (priceImpact > 0.01) {
            const opportunity = {
                type: 'frontrun',
                targetTx: pendingTx,
                expectedProfit: this.estimateProfit(amountIn, priceImpact, poolState),
                gasEstimate: 5000,
                poolAddress,
                tokenA: poolState.tokenA,
                tokenB: poolState.tokenB,
                amountIn: new anchor_1.BN(amountIn),
                amountOut: new anchor_1.BN(minAmountOut),
                priceImpact,
                slippage: 0.01,
                priority: this.calculatePriority(priceImpact, amountIn),
                deadline: Date.now() + 1000
            };
            if (opportunity.expectedProfit > this.config.minProfitThreshold) {
                this.metrics.opportunitiesFound++;
                this.emit('opportunity', opportunity);
            }
        }
    }
    async analyzeOrcaWhirlpoolTx(pendingTx) {
        const data = pendingTx.data;
        if (data.length < 32)
            return;
        const discriminator = data.slice(0, 8).toString('hex');
        // Orca swap discriminator
        if (discriminator !== 'f8c69e91e17587c8')
            return;
        const poolAddress = pendingTx.accounts[2];
        const poolState = await this.getPoolState(poolAddress);
        if (!poolState)
            return;
        const amountIn = this.decodeAmount(data.slice(8, 16));
        const priceImpact = this.calculatePriceImpact(amountIn, poolState.reserveA, poolState.reserveB, poolState.fee);
        if (priceImpact > 0.015) {
            const opportunity = {
                type: 'sandwich',
                targetTx: pendingTx,
                expectedProfit: this.estimateProfit(amountIn, priceImpact, poolState),
                gasEstimate: 6000,
                poolAddress,
                tokenA: poolState.tokenA,
                tokenB: poolState.tokenB,
                amountIn: new anchor_1.BN(amountIn),
                amountOut: new anchor_1.BN(0),
                priceImpact,
                slippage: 0.005,
                priority: this.calculatePriority(priceImpact, amountIn),
                deadline: Date.now() + 800
            };
            if (opportunity.expectedProfit > this.config.minProfitThreshold) {
                this.metrics.opportunitiesFound++;
                this.emit('opportunity', opportunity);
            }
        }
    }
    async analyzeOrcaV2Tx(pendingTx) {
        const data = pendingTx.data;
        if (data.length < 32)
            return;
        const instructionId = data.readUInt8(0);
        // Orca V2 swap instruction
        if (instructionId !== 1)
            return;
        const poolAddress = pendingTx.accounts[1];
        const poolState = await this.getPoolState(poolAddress);
        if (!poolState)
            return;
        const amountIn = this.decodeAmount(data.slice(1, 9));
        const minAmountOut = this.decodeAmount(data.slice(9, 17));
        const priceImpact = this.calculatePriceImpact(amountIn, poolState.reserveA, poolState.reserveB, poolState.fee);
        if (priceImpact > 0.01) {
            const opportunity = {
                type: 'frontrun',
                targetTx: pendingTx,
                expectedProfit: this.estimateProfit(amountIn, priceImpact, poolState),
                gasEstimate: 5500,
                poolAddress,
                tokenA: poolState.tokenA,
                tokenB: poolState.tokenB,
                amountIn: new anchor_1.BN(amountIn),
                amountOut: new anchor_1.BN(minAmountOut),
                priceImpact,
                slippage: 0.01,
                priority: this.calculatePriority(priceImpact, amountIn),
                deadline: Date.now() + 900
            };
            if (opportunity.expectedProfit > this.config.minProfitThreshold) {
                this.metrics.opportunitiesFound++;
                this.emit('opportunity', opportunity);
            }
        }
    }
    async analyzeSerumTx(pendingTx) {
        const data = pendingTx.data;
        if (data.length < 16)
            return;
        const version = data.readUInt8(0);
        const instructionCode = data.readUInt16LE(1);
        // Serum new order instruction
        if (instructionCode !== 0)
            return;
        const marketAddress = pendingTx.accounts[0];
        const poolState = await this.getPoolState(marketAddress);
        if (!poolState)
            return;
        const side = data.readUInt32LE(4);
        const limitPrice = this.decodeAmount(data.slice(8, 16));
        const maxQuantity = this.decodeAmount(data.slice(16, 24));
        const totalValue = (limitPrice * maxQuantity) / 1e9;
        const currentPrice = Number(poolState.reserveB) / Number(poolState.reserveA);
        const priceDeviation = Math.abs((limitPrice / 1e9 - currentPrice) / currentPrice);
        if (priceDeviation > 0.02 && totalValue > 1000) {
            const opportunity = {
                type: 'backrun',
                targetTx: pendingTx,
                expectedProfit: this.estimateSerumProfit(limitPrice, maxQuantity, currentPrice),
                gasEstimate: 7000,
                poolAddress: marketAddress,
                tokenA: poolState.tokenA,
                tokenB: poolState.tokenB,
                amountIn: new anchor_1.BN(maxQuantity),
                amountOut: new anchor_1.BN(limitPrice * maxQuantity / 1e9),
                priceImpact: priceDeviation,
                slippage: 0.005,
                priority: this.calculatePriority(priceDeviation, totalValue),
                deadline: Date.now() + 1200
            };
            if (opportunity.expectedProfit > this.config.minProfitThreshold) {
                this.metrics.opportunitiesFound++;
                this.emit('opportunity', opportunity);
            }
        }
    }
    async getPoolState(poolAddress) {
        const cacheKey = poolAddress.toString();
        const cached = this.poolCache.get(cacheKey);
        if (cached && Date.now() - cached.lastUpdate < 5000) {
            return cached;
        }
        try {
            const accountInfo = await this.connection.getAccountInfo(poolAddress);
            if (!accountInfo)
                return null;
            const poolState = this.decodePoolData(accountInfo.data, poolAddress);
            if (poolState) {
                this.poolCache.set(cacheKey, poolState);
            }
            return poolState;
        }
        catch (error) {
            return null;
        }
    }
    decodePoolData(data, poolAddress) {
        try {
            // Determine DEX type by data length and structure
            const dataLength = data.length;
            // Raydium V4 pool layout
            if (dataLength === 752) {
                const status = data.readBigUInt64LE(0);
                const nonce = data.readUInt8(8);
                const maxOrder = data.readUInt8(9);
                const depth = data.readUInt8(10);
                const baseDecimal = data.readUInt8(11);
                const quoteDecimal = data.readUInt8(12);
                const state = data.readUInt8(13);
                const resetFlag = data.readUInt8(14);
                const minSize = data.readBigUInt64LE(15);
                const volMaxCutRatio = data.readUInt16LE(23);
                const amountWaveRatio = data.readUInt16LE(25);
                const baseLotSize = data.readBigUInt64LE(27);
                const quoteLotSize = data.readBigUInt64LE(35);
                const minPriceMultiplier = data.readBigUInt64LE(43);
                const maxPriceMultiplier = data.readBigUInt64LE(51);
                const systemDecimalValue = data.readBigUInt64LE(59);
                const minSeparateNumerator = data.readBigUInt64LE(67);
                const minSeparateDenominator = data.readBigUInt64LE(75);
                const tradeFeeNumerator = data.readBigUInt64LE(83);
                const tradeFeeDenominator = data.readBigUInt64LE(91);
                const pnlNumerator = data.readBigUInt64LE(99);
                const pnlDenominator = data.readBigUInt64LE(107);
                const swapFeeNumerator = data.readBigUInt64LE(115);
                const swapFeeDenominator = data.readBigUInt64LE(123);
                const baseNeedTakePnl = data.readBigUInt64LE(131);
                const quoteNeedTakePnl = data.readBigUInt64LE(139);
                const quoteTotalPnl = data.readBigUInt64LE(147);
                const baseTotalPnl = data.readBigUInt64LE(155);
                const poolOpenTime = data.readBigUInt64LE(163);
                const punishPcAmount = data.readBigUInt64LE(171);
                const punishCoinAmount = data.readBigUInt64LE(179);
                const orderbookToInitTime = data.readBigUInt64LE(187);
                const swapBaseInAmount = Buffer.from(data.slice(195, 211));
                const swapQuoteOutAmount = Buffer.from(data.slice(211, 227));
                const swapBase2QuoteFee = data.readBigUInt64LE(227);
                const swapQuoteInAmount = Buffer.from(data.slice(235, 251));
                const swapBaseOutAmount = Buffer.from(data.slice(251, 267));
                const swapQuote2BaseFee = data.readBigUInt64LE(267);
                const baseVault = new web3_js_1.PublicKey(data.slice(275, 307));
                const quoteVault = new web3_js_1.PublicKey(data.slice(307, 339));
                const baseMint = new web3_js_1.PublicKey(data.slice(339, 371));
                const quoteMint = new web3_js_1.PublicKey(data.slice(371, 403));
                const lpMint = new web3_js_1.PublicKey(data.slice(403, 435));
                const openOrders = new web3_js_1.PublicKey(data.slice(435, 467));
                const marketId = new web3_js_1.PublicKey(data.slice(467, 499));
                const marketProgramId = new web3_js_1.PublicKey(data.slice(499, 531));
                const targetOrders = new web3_js_1.PublicKey(data.slice(531, 563));
                const withdrawQueue = new web3_js_1.PublicKey(data.slice(563, 595));
                const lpVault = new web3_js_1.PublicKey(data.slice(595, 627));
                const owner = new web3_js_1.PublicKey(data.slice(627, 659));
                const lpReserve = data.readBigUInt64LE(659);
                const baseReserve = new anchor_1.BN(data.slice(667, 675), 'le');
                const quoteReserve = new anchor_1.BN(data.slice(675, 683), 'le');
                return {
                    address: poolAddress,
                    tokenA: baseMint,
                    tokenB: quoteMint,
                    reserveA: baseReserve,
                    reserveB: quoteReserve,
                    fee: Number(swapFeeNumerator) / Number(swapFeeDenominator),
                    lastUpdate: Date.now()
                };
            }
            // Orca Whirlpool layout
            else if (dataLength === 653) {
                const whirlpoolsConfig = new web3_js_1.PublicKey(data.slice(0, 32));
                const whirlpoolBump = data[32];
                const tickSpacing = data.readUInt16LE(33);
                const tickSpacingSeed = data.slice(35, 37);
                const feeRate = data.readUInt16LE(37);
                const protocolFeeRate = data.readUInt16LE(39);
                const liquidity = Buffer.from(data.slice(41, 57));
                const sqrtPrice = Buffer.from(data.slice(57, 73));
                const tickCurrentIndex = data.readInt32LE(73);
                const protocolFeeOwedA = data.readBigUInt64LE(77);
                const protocolFeeOwedB = data.readBigUInt64LE(85);
                const tokenMintA = new web3_js_1.PublicKey(data.slice(93, 125));
                const tokenVaultA = new web3_js_1.PublicKey(data.slice(125, 157));
                const feeGrowthGlobalA = Buffer.from(data.slice(157, 173));
                const tokenMintB = new web3_js_1.PublicKey(data.slice(173, 205));
                const tokenVaultB = new web3_js_1.PublicKey(data.slice(205, 237));
                const feeGrowthGlobalB = Buffer.from(data.slice(237, 253));
                const rewardLastUpdatedTimestamp = data.readBigUInt64LE(253);
                const rewardInfos = data.slice(261, 589);
                // Get actual reserves from vault accounts
                const reserveA = new anchor_1.BN(liquidity.slice(0, 8), 'le');
                const reserveB = new anchor_1.BN(liquidity.slice(8, 16), 'le');
                return {
                    address: poolAddress,
                    tokenA: tokenMintA,
                    tokenB: tokenMintB,
                    reserveA,
                    reserveB,
                    fee: feeRate / 1000000,
                    lastUpdate: Date.now()
                };
            }
            // Orca V2 pool layout
            else if (dataLength === 324) {
                const version = data.readUInt8(0);
                const bumpSeed = data.readUInt8(1);
                const tokenProgramId = new web3_js_1.PublicKey(data.slice(2, 34));
                const tokenAAccount = new web3_js_1.PublicKey(data.slice(34, 66));
                const tokenBAccount = new web3_js_1.PublicKey(data.slice(66, 98));
                const tokenAMint = new web3_js_1.PublicKey(data.slice(98, 130));
                const tokenBMint = new web3_js_1.PublicKey(data.slice(130, 162));
                const poolMint = new web3_js_1.PublicKey(data.slice(162, 194));
                const feeAccount = new web3_js_1.PublicKey(data.slice(194, 226));
                const tradeFeeNumerator = data.readBigUInt64LE(226);
                const tradeFeeDenominator = data.readBigUInt64LE(234);
                const ownerTradeFeeNumerator = data.readBigUInt64LE(242);
                const ownerTradeFeeDenominator = data.readBigUInt64LE(250);
                const ownerWithdrawFeeNumerator = data.readBigUInt64LE(258);
                const ownerWithdrawFeeDenominator = data.readBigUInt64LE(266);
                const hostFeeNumerator = data.readBigUInt64LE(274);
                const hostFeeDenominator = data.readBigUInt64LE(282);
                const curveType = data.readUInt8(290);
                const curveParameters = data.slice(291, 323);
                // Estimate reserves based on curve parameters
                const reserveA = new anchor_1.BN(curveParameters.slice(0, 8), 'le');
                const reserveB = new anchor_1.BN(curveParameters.slice(8, 16), 'le');
                return {
                    address: poolAddress,
                    tokenA: tokenAMint,
                    tokenB: tokenBMint,
                    reserveA,
                    reserveB,
                    fee: Number(tradeFeeNumerator) / Number(tradeFeeDenominator),
                    lastUpdate: Date.now()
                };
            }
            // Serum market layout (simplified)
            else if (dataLength >= 388) {
                const serumPadding = data.slice(0, 5);
                const accountFlags = data.readBigUInt64LE(5);
                const ownAddress = new web3_js_1.PublicKey(data.slice(13, 45));
                const vaultSignerNonce = data.readBigUInt64LE(45);
                const baseMint = new web3_js_1.PublicKey(data.slice(53, 85));
                const quoteMint = new web3_js_1.PublicKey(data.slice(85, 117));
                const baseVault = new web3_js_1.PublicKey(data.slice(117, 149));
                const baseDepositsTotal = data.readBigUInt64LE(149);
                const baseFeesAccrued = data.readBigUInt64LE(157);
                const quoteVault = new web3_js_1.PublicKey(data.slice(165, 197));
                const quoteDepositsTotal = data.readBigUInt64LE(197);
                const quoteFeesAccrued = data.readBigUInt64LE(205);
                const quoteDustThreshold = data.readBigUInt64LE(213);
                const requestQueue = new web3_js_1.PublicKey(data.slice(221, 253));
                const eventQueue = new web3_js_1.PublicKey(data.slice(253, 285));
                const bids = new web3_js_1.PublicKey(data.slice(285, 317));
                const asks = new web3_js_1.PublicKey(data.slice(317, 349));
                const baseLotSize = data.readBigUInt64LE(349);
                const quoteLotSize = data.readBigUInt64LE(357);
                const feeRateBps = data.readBigUInt64LE(365);
                return {
                    address: poolAddress,
                    tokenA: baseMint,
                    tokenB: quoteMint,
                    reserveA: new anchor_1.BN(baseDepositsTotal.toString()),
                    reserveB: new anchor_1.BN(quoteDepositsTotal.toString()),
                    fee: Number(feeRateBps) / 10000,
                    lastUpdate: Date.now()
                };
            }
            // Generic fallback
            return {
                address: poolAddress,
                tokenA: new web3_js_1.PublicKey(data.slice(0, 32)),
                tokenB: new web3_js_1.PublicKey(data.slice(32, 64)),
                reserveA: new anchor_1.BN(0),
                reserveB: new anchor_1.BN(0),
                fee: 0.003,
                lastUpdate: Date.now()
            };
        }
        catch (error) {
            return null;
        }
    }
    calculatePriceImpact(amountIn, reserveA, reserveB, fee) {
        const reserveANum = Number(reserveA);
        const reserveBNum = Number(reserveB);
        if (reserveANum === 0 || reserveBNum === 0)
            return 0;
        const amountInWithFee = amountIn * (1 - fee);
        const numerator = amountInWithFee * reserveBNum;
        const denominator = reserveANum + amountInWithFee;
        const amountOut = numerator / denominator;
        const spotPrice = reserveBNum / reserveANum;
        const executionPrice = amountOut / amountIn;
        const priceImpact = Math.abs((spotPrice - executionPrice) / spotPrice);
        return priceImpact;
    }
    estimateProfit(amountIn, priceImpact, poolState) {
        const potentialArbitrageProfit = amountIn * priceImpact * 0.8;
        const gasEstimate = 0.005 * 1e9;
        const netProfit = potentialArbitrageProfit - gasEstimate;
        return netProfit / 1e9;
    }
    estimateSerumProfit(limitPrice, quantity, currentPrice) {
        const priceDiff = Math.abs(limitPrice / 1e9 - currentPrice);
        const potentialProfit = priceDiff * quantity / 1e9;
        const gasEstimate = 0.007 * 1e9;
        return (potentialProfit - gasEstimate) / 1e9;
    }
    calculatePriority(priceImpact, value) {
        const impactScore = Math.min(priceImpact * 100, 10);
        const valueScore = Math.log10(value + 1);
        const timeScore = 10 - (Date.now() - this.lastBlockTime) / 100;
        return impactScore * 0.5 + valueScore * 0.3 + Math.max(timeScore, 0) * 0.2;
    }
    decodeAmount(buffer) {
        try {
            return Number(buffer.readBigUInt64LE(0));
        }
        catch {
            return 0;
        }
    }
    updateSlotInfo(params) {
        const { parent, root, slot } = params.result;
        this.lastBlockTime = Date.now();
        this.emit('slot', { slot, timestamp: this.lastBlockTime });
    }
    async initializePoolCache() {
        const knownPools = [
            '58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2', // SOL/USDC Raydium
            'EGZ7tiLeH62TPV1gL8WwbXGzEPa9zmcpVnnkPKKnrE2U', // SOL/USDT Raydium
            '7XawhbbxtsRcQA8KTkHT9f9nc6d69UwqCDh6U5EEbEmX', // SOL/USDC Orca
            '2ZnVuidTHpi5WWKUwFXauYGhvdT9jRKYv5MDahtbwtYr', // SOL/USDT Orca
            'HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ', // SOL/USDC Whirlpool
            'PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY', // SOL/mSOL Marinade
            'AHhRjqvDZDUhdGW2R7QmFnMaHBhS3mPEAuEqhB2MZVgh', // RAY/USDC Raydium
            'DVa7Qmb5ct9RCpaU7UTpSYQfzmdGqPMjfvGUXFnTJvFE', // SOL/stSOL Lido
            'GZ3WBFsqntmERPwumFEYgrX2B7J7G11MzNZAy7Hje27X', // ORCA/USDC Orca
            'FpCMFDFGYotvufJ83mUqj4eZXdboJhQ5d31ep1VWFfuz' // SOL/BONK Raydium
        ];
        const promises = knownPools.map(async (poolAddress) => {
            try {
                await this.getPoolState(new web3_js_1.PublicKey(poolAddress));
            }
            catch (error) {
                // Continue with other pools silently
            }
        });
        await Promise.all(promises);
    }
    handleDisconnection() {
        this.emit('disconnected');
        if (this.isRunning) {
            this.handleReconnection();
        }
    }
    async handleReconnection() {
        if (this.reconnectAttempts >= this.config.maxReconnectAttempts) {
            this.emit('error', new Error('Max reconnection attempts reached'));
            await this.stop();
            return;
        }
        this.reconnectAttempts++;
        const delay = this.config.reconnectDelayMs * Math.pow(2, this.reconnectAttempts - 1);
        setTimeout(() => {
            if (this.isRunning) {
                this.connectWebSocket();
            }
        }, delay);
    }
    startMetricsReporting() {
        this.metricsInterval = setInterval(() => {
            const uptime = (Date.now() - this.metrics.uptime) / 1000;
            const successRate = this.metrics.opportunitiesFound / Math.max(this.metrics.txProcessed, 1);
            this.emit('metrics', {
                ...this.metrics,
                uptime,
                successRate,
                txPerSecond: this.metrics.txProcessed / uptime,
                avgLatencyMs: this.metrics.avgLatency,
                cacheSize: this.poolCache.size,
                queueLength: this.pendingTxQueue.length
            });
            // Clean up old data periodically
            if (this.seenSignatures.size > 50000) {
                const toDelete = Array.from(this.seenSignatures).slice(0, 25000);
                toDelete.forEach(sig => this.seenSignatures.delete(sig));
            }
            // Update pool cache for stale entries
            const staleTime = Date.now() - 30000; // 30 seconds
            for (const [key, pool] of this.poolCache.entries()) {
                if (pool.lastUpdate < staleTime) {
                    this.poolCache.delete(key);
                }
            }
        }, 5000);
    }
    getMetrics() {
        const uptime = (Date.now() - this.metrics.uptime) / 1000;
        return {
            ...this.metrics,
            uptime,
            successRate: this.metrics.opportunitiesFound / Math.max(this.metrics.txProcessed, 1),
            txPerSecond: this.metrics.txProcessed / uptime,
            cacheHitRate: this.poolCache.size > 0 ? 1 - (this.metrics.errors / this.metrics.txProcessed) : 0
        };
    }
    clearCache() {
        this.poolCache.clear();
        this.seenSignatures.clear();
        this.pendingTxQueue = [];
    }
    updateConfig(config) {
        const wasRunning = this.isRunning;
        if (wasRunning && (config.rpcEndpoint || config.wsEndpoint)) {
            this.stop().then(() => {
                this.config = { ...this.config, ...config };
                this.connection = new web3_js_1.Connection(this.config.rpcEndpoint, {
                    commitment: this.config.commitment,
                    wsEndpoint: this.config.wsEndpoint
                });
                this.start();
            });
        }
        else {
            this.config = { ...this.config, ...config };
        }
    }
    getPoolCache() {
        return new Map(this.poolCache);
    }
    getQueueLength() {
        return this.pendingTxQueue.length;
    }
    isConnected() {
        return this.ws !== null && this.ws.readyState === ws_1.default.OPEN;
    }
    async testConnection() {
        try {
            const slot = await this.connection.getSlot();
            return slot > 0;
        }
        catch {
            return false;
        }
    }
    on(event, listener) {
        return super.on(event, listener);
    }
    removeAllListeners(event) {
        return super.removeAllListeners(event);
    }
}
exports.PendingTxMonitor = PendingTxMonitor;
exports.default = PendingTxMonitor;
//# sourceMappingURL=pendingTxMonitor.js.map