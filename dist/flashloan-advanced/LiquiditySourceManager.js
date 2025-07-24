"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.LiquiditySourceManager = exports.ProtocolType = void 0;
const web3_js_1 = require("@solana/web3.js");
const borsh_1 = require("@project-serum/borsh");
const anchor_1 = require("@project-serum/anchor");
const events_1 = require("events");
const pino_1 = __importDefault(require("pino"));
const logger = (0, pino_1.default)({ level: process.env.LOG_LEVEL || 'info' });
var ProtocolType;
(function (ProtocolType) {
    ProtocolType["RAYDIUM"] = "RAYDIUM";
    ProtocolType["ORCA"] = "ORCA";
    ProtocolType["SABER"] = "SABER";
    ProtocolType["MERCURIAL"] = "MERCURIAL";
    ProtocolType["SERUM"] = "SERUM";
    ProtocolType["PHOENIX"] = "PHOENIX";
    ProtocolType["LIFINITY"] = "LIFINITY";
    ProtocolType["MARINADE"] = "MARINADE";
    ProtocolType["ALDRIN"] = "ALDRIN";
    ProtocolType["CREMA"] = "CREMA";
    ProtocolType["CROPPER"] = "CROPPER";
    ProtocolType["SENCHA"] = "SENCHA";
    ProtocolType["SAROS"] = "SAROS";
    ProtocolType["STEPN"] = "STEPN";
    ProtocolType["INVARIANT"] = "INVARIANT";
    ProtocolType["METEORA"] = "METEORA";
    ProtocolType["GOOSEFX"] = "GOOSEFX";
    ProtocolType["HELIUM"] = "HELIUM";
})(ProtocolType || (exports.ProtocolType = ProtocolType = {}));
const POOL_LAYOUTS = {
    [ProtocolType.RAYDIUM]: (0, borsh_1.struct)([
        (0, borsh_1.u64)("status"),
        (0, borsh_1.u64)("nonce"),
        (0, borsh_1.u64)("maxOrder"),
        (0, borsh_1.u64)("depth"),
        (0, borsh_1.u64)("baseDecimal"),
        (0, borsh_1.u64)("quoteDecimal"),
        (0, borsh_1.u64)("state"),
        (0, borsh_1.u64)("resetFlag"),
        (0, borsh_1.u64)("minSize"),
        (0, borsh_1.u64)("volMaxCutRatio"),
        (0, borsh_1.u64)("amountWaveRatio"),
        (0, borsh_1.u64)("baseLotSize"),
        (0, borsh_1.u64)("quoteLotSize"),
        (0, borsh_1.u64)("minPriceMultiplier"),
        (0, borsh_1.u64)("maxPriceMultiplier"),
        (0, borsh_1.u64)("systemDecimalValue"),
        (0, borsh_1.u64)("minSeparateNumerator"),
        (0, borsh_1.u64)("minSeparateDenominator"),
        (0, borsh_1.u64)("tradeFeeNumerator"),
        (0, borsh_1.u64)("tradeFeeDenominator"),
        (0, borsh_1.u64)("pnlNumerator"),
        (0, borsh_1.u64)("pnlDenominator"),
        (0, borsh_1.u64)("swapFeeNumerator"),
        (0, borsh_1.u64)("swapFeeDenominator"),
        (0, borsh_1.u64)("baseNeedTakePnl"),
        (0, borsh_1.u64)("quoteNeedTakePnl"),
        (0, borsh_1.u64)("quoteTotalPnl"),
        (0, borsh_1.u64)("baseTotalPnl"),
        (0, borsh_1.u128)("quoteTotalDeposited"),
        (0, borsh_1.u128)("baseTotalDeposited"),
        (0, borsh_1.u128)("swapBaseInAmount"),
        (0, borsh_1.u128)("swapQuoteOutAmount"),
        (0, borsh_1.u64)("swapBase2QuoteFee"),
        (0, borsh_1.u128)("swapQuoteInAmount"),
        (0, borsh_1.u128)("swapBaseOutAmount"),
        (0, borsh_1.u64)("swapQuote2BaseFee"),
        (0, borsh_1.publicKey)("baseVault"),
        (0, borsh_1.publicKey)("quoteVault"),
        (0, borsh_1.publicKey)("baseMint"),
        (0, borsh_1.publicKey)("quoteMint"),
        (0, borsh_1.publicKey)("lpMint"),
        (0, borsh_1.publicKey)("openOrders"),
        (0, borsh_1.publicKey)("marketId"),
        (0, borsh_1.publicKey)("marketProgramId"),
        (0, borsh_1.publicKey)("targetOrders"),
        (0, borsh_1.publicKey)("withdrawQueue"),
        (0, borsh_1.publicKey)("lpVault"),
        (0, borsh_1.publicKey)("owner"),
        (0, borsh_1.publicKey)("pnlOwner"),
    ]),
    [ProtocolType.ORCA]: null,
    [ProtocolType.SABER]: null,
    [ProtocolType.MERCURIAL]: null,
    [ProtocolType.SERUM]: null,
    [ProtocolType.PHOENIX]: null,
    [ProtocolType.LIFINITY]: null,
    [ProtocolType.MARINADE]: null,
    [ProtocolType.ALDRIN]: null,
    [ProtocolType.CREMA]: null,
    [ProtocolType.CROPPER]: null,
    [ProtocolType.SENCHA]: null,
    [ProtocolType.SAROS]: null,
    [ProtocolType.STEPN]: null,
    [ProtocolType.INVARIANT]: null,
    [ProtocolType.METEORA]: null,
    [ProtocolType.GOOSEFX]: null,
    [ProtocolType.HELIUM]: null
};
class LiquiditySourceManager extends events_1.EventEmitter {
    constructor(connection) {
        super();
        this.updateInterval = null;
        this.CACHE_TTL = 1000;
        this.MAX_ROUTE_HOPS = 3;
        this.MIN_LIQUIDITY_USD = 10000;
        this.MAX_PRICE_IMPACT = 0.03;
        this.subscriptions = new Map();
        this.connection = connection;
        this.sources = new Map();
        this.poolCache = new Map();
        this.tokenPairIndex = new Map();
        this.initializeSources();
    }
    initializeSources() {
        const sourceConfigs = [
            {
                protocol: ProtocolType.RAYDIUM,
                programId: new web3_js_1.PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'),
                feeStructure: { baseFee: 0.0025, protocolFee: 0.0001, lpFee: 0.0024 },
                priority: 1
            },
            {
                protocol: ProtocolType.ORCA,
                programId: new web3_js_1.PublicKey('9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'),
                feeStructure: { baseFee: 0.003, protocolFee: 0.0001, lpFee: 0.0029 },
                priority: 2
            },
            {
                protocol: ProtocolType.METEORA,
                programId: new web3_js_1.PublicKey('LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo'),
                feeStructure: { baseFee: 0.002, protocolFee: 0.0001, lpFee: 0.0019 },
                priority: 3
            },
            {
                protocol: ProtocolType.PHOENIX,
                programId: new web3_js_1.PublicKey('PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY'),
                feeStructure: { baseFee: 0.001, protocolFee: 0.0001, lpFee: 0.0009 },
                priority: 4
            },
            {
                protocol: ProtocolType.LIFINITY,
                programId: new web3_js_1.PublicKey('EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S'),
                feeStructure: { baseFee: 0.002, protocolFee: 0.0001, lpFee: 0.0019 },
                priority: 5
            }
        ];
        sourceConfigs.forEach(config => {
            this.sources.set(config.protocol, {
                protocol: config.protocol,
                programId: config.programId,
                pools: new Map(),
                feeStructure: config.feeStructure,
                priority: config.priority,
                isActive: true,
                lastSync: 0
            });
        });
    }
    async initialize() {
        try {
            await this.syncAllPools();
            this.startPoolUpdates();
            logger.info('LiquiditySourceManager initialized successfully');
        }
        catch (error) {
            logger.error('Failed to initialize LiquiditySourceManager:', error);
            throw error;
        }
    }
    async syncAllPools() {
        const syncPromises = Array.from(this.sources.values())
            .filter(source => source.isActive)
            .map(source => this.syncPoolsForSource(source));
        await Promise.allSettled(syncPromises);
    }
    async syncPoolsForSource(source) {
        try {
            const accounts = await this.connection.getProgramAccounts(source.programId, {
                commitment: 'confirmed',
                filters: [{ dataSize: this.getPoolDataSize(source.protocol) }]
            });
            const validPools = await Promise.all(accounts.map(async ({ pubkey, account }) => {
                try {
                    const pool = await this.parsePoolData(pubkey, account, source.protocol);
                    if (pool && pool.liquidity.gt(new anchor_1.BN(this.MIN_LIQUIDITY_USD))) {
                        return pool;
                    }
                    return null;
                }
                catch {
                    return null;
                }
            }));
            validPools.forEach(pool => {
                if (pool) {
                    const poolKey = pool.address.toBase58();
                    source.pools.set(poolKey, pool);
                    this.poolCache.set(poolKey, pool);
                    this.updateTokenPairIndex(pool);
                }
            });
            source.lastSync = Date.now();
            logger.info(`Synced ${source.pools.size} pools for ${source.protocol}`);
        }
        catch (error) {
            logger.error(`Failed to sync pools for ${source.protocol}:`, error);
        }
    }
    getPoolDataSize(protocol) {
        const sizes = {
            [ProtocolType.RAYDIUM]: 752,
            [ProtocolType.ORCA]: 324,
            [ProtocolType.SABER]: 280,
            [ProtocolType.MERCURIAL]: 368,
            [ProtocolType.SERUM]: 388,
            [ProtocolType.PHOENIX]: 456,
            [ProtocolType.LIFINITY]: 324,
            [ProtocolType.MARINADE]: 280,
            [ProtocolType.ALDRIN]: 312,
            [ProtocolType.CREMA]: 296,
            [ProtocolType.CROPPER]: 280,
            [ProtocolType.SENCHA]: 264,
            [ProtocolType.SAROS]: 280,
            [ProtocolType.STEPN]: 296,
            [ProtocolType.INVARIANT]: 384,
            [ProtocolType.METEORA]: 356,
            [ProtocolType.GOOSEFX]: 392,
            [ProtocolType.HELIUM]: 280
        };
        return sizes[protocol] || 280;
    }
    async parsePoolData(address, account, protocol) {
        try {
            const layout = POOL_LAYOUTS[protocol];
            if (!layout)
                return null;
            const poolData = layout.decode(account.data);
            const [tokenABalance, tokenBBalance] = await this.getPoolBalances(poolData, protocol);
            return {
                address,
                protocol,
                tokenA: poolData.baseMint,
                tokenB: poolData.quoteMint,
                reserveA: new anchor_1.BN(tokenABalance),
                reserveB: new anchor_1.BN(tokenBBalance),
                fee: this.calculatePoolFee(poolData, protocol),
                lastUpdate: Date.now(),
                liquidity: this.calculateLiquidity(tokenABalance, tokenBBalance),
                volume24h: new anchor_1.BN(0),
                apy: 0
            };
        }
        catch (error) {
            logger.debug(`Failed to parse pool data for ${address.toBase58()}`);
            return null;
        }
    }
    async getPoolBalances(poolData, protocol) {
        try {
            const [baseVaultInfo, quoteVaultInfo] = await Promise.all([
                this.connection.getTokenAccountBalance(poolData.baseVault),
                this.connection.getTokenAccountBalance(poolData.quoteVault)
            ]);
            return [
                baseVaultInfo.value.amount,
                quoteVaultInfo.value.amount
            ];
        }
        catch {
            return ['0', '0'];
        }
    }
    calculatePoolFee(poolData, protocol) {
        if (protocol === ProtocolType.RAYDIUM) {
            return poolData.swapFeeNumerator.toNumber() / poolData.swapFeeDenominator.toNumber();
        }
        const source = this.sources.get(protocol);
        return source?.feeStructure.baseFee || 0.003;
    }
    calculateLiquidity(reserveA, reserveB) {
        const a = new anchor_1.BN(reserveA);
        const b = new anchor_1.BN(reserveB);
        return a.mul(b).div(new anchor_1.BN(2)).mul(new anchor_1.BN(2));
    }
    updateTokenPairIndex(pool) {
        const pairKey = this.getPairKey(pool.tokenA, pool.tokenB);
        if (!this.tokenPairIndex.has(pairKey)) {
            this.tokenPairIndex.set(pairKey, new Set());
        }
        this.tokenPairIndex.get(pairKey).add(pool.address.toBase58());
    }
    getPairKey(tokenA, tokenB) {
        const [first, second] = tokenA.toBase58() < tokenB.toBase58()
            ? [tokenA, tokenB]
            : [tokenB, tokenA];
        return `${first.toBase58()}-${second.toBase58()}`;
    }
    findOptimalRoute(inputToken, outputToken, inputAmount, maxSlippage = 0.01) {
        const directPools = this.getDirectPools(inputToken, outputToken);
        let bestRoute = null;
        // Try direct route first
        if (directPools.length > 0) {
            bestRoute = this.calculateDirectRoute(directPools, inputAmount, maxSlippage);
        }
        // Try multi-hop routes if direct route is not optimal
        if (!bestRoute || bestRoute.priceImpact > this.MAX_PRICE_IMPACT) {
            const multiHopRoute = this.findMultiHopRoute(inputToken, outputToken, inputAmount, maxSlippage);
            if (multiHopRoute && (!bestRoute || multiHopRoute.outputAmount.gt(bestRoute.outputAmount))) {
                bestRoute = multiHopRoute;
            }
        }
        // Try split routes for large trades
        if (inputAmount.gt(new anchor_1.BN('1000000000'))) {
            const splitRoute = this.calculateSplitRoute(directPools, inputAmount, maxSlippage);
            if (splitRoute && (!bestRoute || splitRoute.outputAmount.gt(bestRoute.outputAmount))) {
                bestRoute = splitRoute;
            }
        }
        return bestRoute;
    }
    getDirectPools(tokenA, tokenB) {
        const pairKey = this.getPairKey(tokenA, tokenB);
        const poolAddresses = this.tokenPairIndex.get(pairKey) || new Set();
        return Array.from(poolAddresses)
            .map(addr => this.poolCache.get(addr))
            .filter((pool) => pool !== undefined &&
            pool.liquidity.gt(new anchor_1.BN(this.MIN_LIQUIDITY_USD)) &&
            Date.now() - pool.lastUpdate < this.CACHE_TTL)
            .sort((a, b) => b.liquidity.cmp(a.liquidity));
    }
    calculateDirectRoute(pools, inputAmount, maxSlippage) {
        let bestOutput = new anchor_1.BN(0);
        let bestPool = null;
        let bestPriceImpact = 1;
        for (const pool of pools) {
            const { outputAmount, priceImpact } = this.calculateSwapOutput(pool, inputAmount);
            if (outputAmount.gt(bestOutput) && priceImpact <= maxSlippage) {
                bestOutput = outputAmount;
                bestPool = pool;
                bestPriceImpact = priceImpact;
            }
        }
        if (!bestPool)
            return null;
        const fee = inputAmount.mul(new anchor_1.BN(Math.floor(bestPool.fee * 1e9))).div(new anchor_1.BN(1e9));
        return {
            steps: [{
                    pool: bestPool,
                    inputAmount,
                    outputAmount: bestOutput,
                    priceImpact: bestPriceImpact,
                    fee
                }],
            inputAmount,
            outputAmount: bestOutput,
            totalFees: fee,
            priceImpact: bestPriceImpact,
            executionPrice: bestOutput.mul(new anchor_1.BN(1e9)).div(inputAmount).toNumber() / 1e9
        };
    }
    calculateSwapOutput(pool, inputAmount) {
        const inputReserve = pool.reserveA;
        const outputReserve = pool.reserveB;
        const inputAmountWithFee = inputAmount.mul(new anchor_1.BN(10000 - Math.floor(pool.fee * 10000))).div(new anchor_1.BN(10000));
        const numerator = inputAmountWithFee.mul(outputReserve);
        const denominator = inputReserve.add(inputAmountWithFee);
        const outputAmount = numerator.div(denominator);
        const executionPrice = outputAmount.mul(new anchor_1.BN(1e9)).div(inputAmount).toNumber() / 1e9;
        const spotPrice = outputReserve.mul(new anchor_1.BN(1e9)).div(inputReserve).toNumber() / 1e9;
        const priceImpact = Math.abs(1 - executionPrice / spotPrice);
        return { outputAmount, priceImpact };
    }
    findMultiHopRoute(inputToken, outputToken, inputAmount, maxSlippage) {
        const visited = new Set();
        const queue = [{
                token: inputToken,
                amount: inputAmount,
                path: [],
                totalFees: new anchor_1.BN(0)
            }];
        let bestRoute = null;
        while (queue.length > 0) {
            const current = queue.shift();
            if (current.path.length >= this.MAX_ROUTE_HOPS)
                continue;
            const neighbors = this.getConnectedTokens(current.token);
            for (const nextToken of neighbors) {
                const poolKey = this.getPairKey(current.token, nextToken);
                if (visited.has(poolKey))
                    continue;
                visited.add(poolKey);
                const pools = this.getDirectPools(current.token, nextToken);
                if (pools.length === 0)
                    continue;
                const bestPool = pools[0];
                const { outputAmount, priceImpact } = this.calculateSwapOutput(bestPool, current.amount);
                if (priceImpact > maxSlippage)
                    continue;
                const fee = current.amount.mul(new anchor_1.BN(Math.floor(bestPool.fee * 1e9))).div(new anchor_1.BN(1e9));
                const step = {
                    pool: bestPool,
                    inputAmount: current.amount,
                    outputAmount,
                    priceImpact,
                    fee
                };
                const newPath = [...current.path, step];
                const newTotalFees = current.totalFees.add(fee);
                if (nextToken.equals(outputToken)) {
                    const totalPriceImpact = newPath.reduce((sum, s) => sum + s.priceImpact, 0);
                    if (!bestRoute || outputAmount.gt(bestRoute.outputAmount)) {
                        bestRoute = {
                            steps: newPath,
                            inputAmount,
                            outputAmount,
                            totalFees: newTotalFees,
                            priceImpact: totalPriceImpact,
                            executionPrice: outputAmount.mul(new anchor_1.BN(1e9)).div(inputAmount).toNumber() / 1e9
                        };
                    }
                }
                else {
                    queue.push({
                        token: nextToken,
                        amount: outputAmount,
                        path: newPath,
                        totalFees: newTotalFees
                    });
                }
            }
        }
        return bestRoute;
    }
    getConnectedTokens(token) {
        const connected = new Set();
        for (const [pairKey, _] of this.tokenPairIndex) {
            const [tokenA, tokenB] = pairKey.split('-');
            if (tokenA === token.toBase58()) {
                connected.add(tokenB);
            }
            else if (tokenB === token.toBase58()) {
                connected.add(tokenA);
            }
        }
        return Array.from(connected).map(addr => new web3_js_1.PublicKey(addr));
    }
    calculateSplitRoute(pools, inputAmount, maxSlippage) {
        const splitCount = Math.min(pools.length, 5);
        if (splitCount < 2)
            return null;
        const amountPerSplit = inputAmount.div(new anchor_1.BN(splitCount));
        const steps = [];
        let totalOutput = new anchor_1.BN(0);
        let totalFees = new anchor_1.BN(0);
        let totalPriceImpact = 0;
        for (let i = 0; i < splitCount; i++) {
            const pool = pools[i];
            const splitAmount = i === splitCount - 1
                ? inputAmount.sub(amountPerSplit.mul(new anchor_1.BN(splitCount - 1)))
                : amountPerSplit;
            const { outputAmount, priceImpact } = this.calculateSwapOutput(pool, splitAmount);
            if (priceImpact > maxSlippage)
                return null;
            const fee = splitAmount.mul(new anchor_1.BN(Math.floor(pool.fee * 1e9))).div(new anchor_1.BN(1e9));
            steps.push({
                pool,
                inputAmount: splitAmount,
                outputAmount,
                priceImpact,
                fee
            });
            totalOutput = totalOutput.add(outputAmount);
            totalFees = totalFees.add(fee);
            totalPriceImpact += priceImpact * (splitAmount.toNumber() / inputAmount.toNumber());
        }
        return {
            steps,
            inputAmount,
            outputAmount: totalOutput,
            totalFees,
            priceImpact: totalPriceImpact,
            executionPrice: totalOutput.mul(new anchor_1.BN(1e9)).div(inputAmount).toNumber() / 1e9
        };
    }
    async subscribeToPool(poolAddress) {
        const poolKey = poolAddress.toBase58();
        if (this.subscriptions.has(poolKey))
            return;
        const subscriptionId = this.connection.onAccountChange(poolAddress, async (accountInfo) => {
            try {
                const pool = this.poolCache.get(poolKey);
                if (!pool)
                    return;
                const updatedPool = await this.parsePoolData(poolAddress, accountInfo, pool.protocol);
                if (updatedPool) {
                    this.poolCache.set(poolKey, updatedPool);
                    const source = this.sources.get(pool.protocol);
                    if (source) {
                        source.pools.set(poolKey, updatedPool);
                    }
                    this.emit('poolUpdate', updatedPool);
                }
            }
            catch (error) {
                logger.error(`Error updating pool ${poolKey}:`, error);
            }
        }, 'confirmed');
        this.subscriptions.set(poolKey, subscriptionId);
    }
    async unsubscribeFromPool(poolAddress) {
        const poolKey = poolAddress.toBase58();
        const subscriptionId = this.subscriptions.get(poolKey);
        if (subscriptionId !== undefined) {
            await this.connection.removeAccountChangeListener(subscriptionId);
            this.subscriptions.delete(poolKey);
        }
    }
    startPoolUpdates() {
        this.updateInterval = setInterval(async () => {
            try {
                const updatePromises = Array.from(this.sources.values())
                    .filter(source => source.isActive && Date.now() - source.lastSync > 30000)
                    .map(source => this.syncPoolsForSource(source));
                await Promise.allSettled(updatePromises);
            }
            catch (error) {
                logger.error('Error in pool update cycle:', error);
            }
        }, 5000);
    }
    getPoolsByToken(token) {
        const pools = [];
        for (const [pairKey, poolAddresses] of this.tokenPairIndex) {
            if (pairKey.includes(token.toBase58())) {
                for (const addr of poolAddresses) {
                    const pool = this.poolCache.get(addr);
                    if (pool)
                        pools.push(pool);
                }
            }
        }
        return pools.sort((a, b) => b.liquidity.cmp(a.liquidity));
    }
    getTopPools(limit = 100) {
        return Array.from(this.poolCache.values())
            .sort((a, b) => b.liquidity.cmp(a.liquidity))
            .slice(0, limit);
    }
    async updatePoolMetrics(poolAddress, volume24h, apy) {
        const pool = this.poolCache.get(poolAddress.toBase58());
        if (pool) {
            pool.volume24h = volume24h;
            pool.apy = apy;
            this.emit('poolMetricsUpdate', pool);
        }
    }
    getTotalLiquidity() {
        return Array.from(this.poolCache.values())
            .reduce((total, pool) => total.add(pool.liquidity), new anchor_1.BN(0));
    }
    getActiveProtocols() {
        return Array.from(this.sources.values())
            .filter(source => source.isActive)
            .map(source => source.protocol);
    }
    async shutdown() {
        if (this.updateInterval) {
            clearInterval(this.updateInterval);
            this.updateInterval = null;
        }
        const unsubscribePromises = Array.from(this.subscriptions.keys())
            .map(poolKey => this.unsubscribeFromPool(new web3_js_1.PublicKey(poolKey)));
        await Promise.all(unsubscribePromises);
        this.removeAllListeners();
        logger.info('LiquiditySourceManager shutdown complete');
    }
    getPoolByAddress(address) {
        return this.poolCache.get(address.toBase58());
    }
    clearCache() {
        this.poolCache.clear();
        this.tokenPairIndex.clear();
        for (const source of this.sources.values()) {
            source.pools.clear();
            source.lastSync = 0;
        }
    }
    async refreshPool(poolAddress) {
        try {
            const pool = this.poolCache.get(poolAddress.toBase58());
            if (!pool)
                return null;
            const accountInfo = await this.connection.getAccountInfo(poolAddress, 'confirmed');
            if (!accountInfo) {
                this.removePool(poolAddress);
                return null;
            }
            const updatedPool = await this.parsePoolData(poolAddress, accountInfo, pool.protocol);
            if (updatedPool) {
                this.poolCache.set(poolAddress.toBase58(), updatedPool);
                const source = this.sources.get(pool.protocol);
                if (source) {
                    source.pools.set(poolAddress.toBase58(), updatedPool);
                }
                this.emit('poolRefreshed', updatedPool);
            }
            return updatedPool;
        }
        catch (error) {
            logger.error(`Failed to refresh pool ${poolAddress.toBase58()}:`, error);
            return null;
        }
    }
    removePool(poolAddress) {
        const poolKey = poolAddress.toBase58();
        const pool = this.poolCache.get(poolKey);
        if (pool) {
            this.poolCache.delete(poolKey);
            const source = this.sources.get(pool.protocol);
            if (source) {
                source.pools.delete(poolKey);
            }
            const pairKey = this.getPairKey(pool.tokenA, pool.tokenB);
            const pairPools = this.tokenPairIndex.get(pairKey);
            if (pairPools) {
                pairPools.delete(poolKey);
                if (pairPools.size === 0) {
                    this.tokenPairIndex.delete(pairKey);
                }
            }
        }
    }
    getHealthStatus() {
        const errors = [];
        const now = Date.now();
        let activeSources = 0;
        for (const source of this.sources.values()) {
            if (source.isActive) {
                activeSources++;
                if (now - source.lastSync > 60000) {
                    errors.push(`${source.protocol} hasn't synced in over 60 seconds`);
                }
            }
        }
        if (activeSources === 0) {
            errors.push('No active liquidity sources');
        }
        if (this.poolCache.size < 10) {
            errors.push('Pool cache has less than 10 pools');
        }
        return {
            isHealthy: errors.length === 0,
            activeSources,
            totalPools: this.poolCache.size,
            lastUpdate: Math.max(...Array.from(this.sources.values()).map(s => s.lastSync)),
            errors
        };
    }
    setSourceActive(protocol, active) {
        const source = this.sources.get(protocol);
        if (source) {
            source.isActive = active;
            if (!active) {
                for (const poolKey of source.pools.keys()) {
                    this.removePool(new web3_js_1.PublicKey(poolKey));
                }
            }
            logger.info(`Set ${protocol} active status to ${active}`);
        }
    }
    getRouteMetrics(route) {
        const effectivePrice = route.executionPrice;
        const priceImpactBps = Math.floor(route.priceImpact * 10000);
        const totalFeeBps = Math.floor(route.totalFees.mul(new anchor_1.BN(10000)).div(route.inputAmount).toNumber());
        const liquidityUsed = route.steps.reduce((sum, step) => sum.add(step.pool.liquidity), new anchor_1.BN(0));
        return {
            effectivePrice,
            priceImpactBps,
            totalFeeBps,
            liquidityUsed
        };
    }
    async batchRefreshPools(poolAddresses) {
        const results = new Map();
        const BATCH_SIZE = 100;
        for (let i = 0; i < poolAddresses.length; i += BATCH_SIZE) {
            const batch = poolAddresses.slice(i, i + BATCH_SIZE);
            const batchResults = await Promise.all(batch.map(async (address) => {
                const pool = await this.refreshPool(address);
                return { address: address.toBase58(), pool };
            }));
            batchResults.forEach(({ address, pool }) => {
                results.set(address, pool);
            });
            // Rate limiting to avoid RPC overload
            if (i + BATCH_SIZE < poolAddresses.length) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }
        return results;
    }
    getProtocolStats(protocol) {
        const source = this.sources.get(protocol);
        if (!source)
            return null;
        const pools = Array.from(source.pools.values());
        const totalLiquidity = pools.reduce((sum, pool) => sum.add(pool.liquidity), new anchor_1.BN(0));
        const totalVolume24h = pools.reduce((sum, pool) => sum.add(pool.volume24h), new anchor_1.BN(0));
        const averageAPY = pools.length > 0
            ? pools.reduce((sum, pool) => sum + pool.apy, 0) / pools.length
            : 0;
        return {
            poolCount: pools.length,
            totalLiquidity,
            totalVolume24h,
            averageAPY,
            topPools: pools.sort((a, b) => b.liquidity.cmp(a.liquidity)).slice(0, 10)
        };
    }
    estimateGasForRoute(route) {
        const BASE_COMPUTE_UNITS = 200000;
        const PER_HOP_COMPUTE_UNITS = 150000;
        const PER_SPLIT_COMPUTE_UNITS = 50000;
        let totalComputeUnits = BASE_COMPUTE_UNITS;
        const uniquePools = new Set(route.steps.map(step => step.pool.address.toBase58()));
        totalComputeUnits += uniquePools.size * PER_HOP_COMPUTE_UNITS;
        if (route.steps.length > uniquePools.size) {
            totalComputeUnits += (route.steps.length - uniquePools.size) * PER_SPLIT_COMPUTE_UNITS;
        }
        return Math.min(totalComputeUnits, 1400000);
    }
    validateRoute(route) {
        const errors = [];
        if (route.steps.length === 0) {
            errors.push('Route has no steps');
        }
        if (route.priceImpact > this.MAX_PRICE_IMPACT) {
            errors.push(`Price impact ${(route.priceImpact * 100).toFixed(2)}% exceeds maximum ${this.MAX_PRICE_IMPACT * 100}%`);
        }
        for (let i = 0; i < route.steps.length; i++) {
            const step = route.steps[i];
            const pool = this.poolCache.get(step.pool.address.toBase58());
            if (!pool) {
                errors.push(`Pool ${step.pool.address.toBase58()} not found in cache`);
                continue;
            }
            if (Date.now() - pool.lastUpdate > this.CACHE_TTL * 2) {
                errors.push(`Pool ${step.pool.address.toBase58()} data is stale`);
            }
            if (step.inputAmount.gt(pool.reserveA.div(new anchor_1.BN(10)))) {
                errors.push(`Input amount exceeds 10% of pool reserves for ${step.pool.address.toBase58()}`);
            }
        }
        return {
            isValid: errors.length === 0,
            errors
        };
    }
    async warmupCache(tokens) {
        logger.info(`Warming up cache for ${tokens.length} tokens`);
        const pairs = new Set();
        for (let i = 0; i < tokens.length; i++) {
            for (let j = i + 1; j < tokens.length; j++) {
                pairs.add(this.getPairKey(tokens[i], tokens[j]));
            }
        }
        const poolsToRefresh = [];
        for (const pairKey of pairs) {
            const poolAddresses = this.tokenPairIndex.get(pairKey);
            if (poolAddresses) {
                poolAddresses.forEach(addr => poolsToRefresh.push(new web3_js_1.PublicKey(addr)));
            }
        }
        await this.batchRefreshPools(poolsToRefresh);
        logger.info(`Cache warmed up with ${poolsToRefresh.length} pools`);
    }
    getSlippageAdjustedOutput(route, slippageBps) {
        const slippageMultiplier = new anchor_1.BN(10000 - slippageBps);
        return route.outputAmount.mul(slippageMultiplier).div(new anchor_1.BN(10000));
    }
    async getHistoricalMetrics(poolAddress, duration = 86400000) {
        const pool = this.poolCache.get(poolAddress.toBase58());
        if (!pool)
            return null;
        const endTime = Date.now();
        const startTime = endTime - duration;
        const intervals = Math.min(Math.floor(duration / 300000), 288); // 5-min intervals, max 24h
        try {
            // Fetch historical snapshots from on-chain history account or indexer
            const historyAddress = web3_js_1.PublicKey.findProgramAddressSync([Buffer.from('history'), poolAddress.toBuffer()], pool.protocol === ProtocolType.RAYDIUM
                ? new web3_js_1.PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8')
                : pool.address)[0];
            const historyAccount = await this.connection.getAccountInfo(historyAddress);
            let liquiditySum = new anchor_1.BN(0);
            let volumeTotal = new anchor_1.BN(0);
            let pricePoints = [];
            if (historyAccount && historyAccount.data.length > 0) {
                // Parse historical data based on protocol-specific layout
                const dataSlice = historyAccount.data.slice(-intervals * 48); // 48 bytes per snapshot
                for (let i = 0; i < dataSlice.length; i += 48) {
                    const liquidity = new anchor_1.BN(dataSlice.slice(i, i + 16), 'le');
                    const volume = new anchor_1.BN(dataSlice.slice(i + 16, i + 32), 'le');
                    const price = dataSlice.readDoubleLE(i + 32);
                    liquiditySum = liquiditySum.add(liquidity);
                    volumeTotal = volumeTotal.add(volume);
                    pricePoints.push(price);
                }
            }
            else {
                // Fallback to current data if no history available
                liquiditySum = pool.liquidity.mul(new anchor_1.BN(intervals));
                volumeTotal = pool.volume24h;
                pricePoints = [pool.reserveB.mul(new anchor_1.BN(1e9)).div(pool.reserveA).toNumber() / 1e9];
            }
            const averageLiquidity = liquiditySum.div(new anchor_1.BN(Math.max(intervals, 1)));
            // Calculate price volatility
            let priceVolatility = 0;
            if (pricePoints.length > 1) {
                const mean = pricePoints.reduce((a, b) => a + b, 0) / pricePoints.length;
                const variance = pricePoints.reduce((sum, price) => sum + Math.pow(price - mean, 2), 0) / pricePoints.length;
                priceVolatility = Math.sqrt(variance) / mean;
            }
            return {
                averageLiquidity,
                volumeTotal,
                priceVolatility
            };
        }
        catch (error) {
            logger.error(`Failed to fetch historical metrics for ${poolAddress.toBase58()}:`, error);
            // Return current data as fallback
            return {
                averageLiquidity: pool.liquidity,
                volumeTotal: pool.volume24h,
                priceVolatility: 0.02
            };
        }
    }
    getArbitrageOpportunities(minProfitBps = 10) {
        const opportunities = [];
        for (const [pairKey, poolAddresses] of this.tokenPairIndex) {
            if (poolAddresses.size < 2)
                continue;
            const pools = Array.from(poolAddresses)
                .map(addr => this.poolCache.get(addr))
                .filter((pool) => pool !== undefined);
            for (let i = 0; i < pools.length; i++) {
                for (let j = i + 1; j < pools.length; j++) {
                    const priceA = pools[i].reserveB.mul(new anchor_1.BN(1e9)).div(pools[i].reserveA);
                    const priceB = pools[j].reserveB.mul(new anchor_1.BN(1e9)).div(pools[j].reserveA);
                    const priceDiff = priceA.gt(priceB)
                        ? priceA.sub(priceB).mul(new anchor_1.BN(10000)).div(priceB)
                        : priceB.sub(priceA).mul(new anchor_1.BN(10000)).div(priceA);
                    if (priceDiff.gte(new anchor_1.BN(minProfitBps))) {
                        opportunities.push({
                            tokenA: pools[i].tokenA,
                            tokenB: pools[i].tokenB,
                            buyPool: priceA.gt(priceB) ? pools[j] : pools[i],
                            sellPool: priceA.gt(priceB) ? pools[i] : pools[j],
                            profitBps: priceDiff.toNumber()
                        });
                    }
                }
            }
        }
        return opportunities.sort((a, b) => b.profitBps - a.profitBps);
    }
    async simulateRoute(route, slippageBps = 50) {
        try {
            let currentAmount = route.inputAmount;
            let totalPriceImpact = 0;
            for (const step of route.steps) {
                const pool = await this.refreshPool(step.pool.address);
                if (!pool) {
                    return {
                        success: false,
                        finalOutput: new anchor_1.BN(0),
                        actualPriceImpact: 1,
                        gasEstimate: 0
                    };
                }
                const { outputAmount, priceImpact } = this.calculateSwapOutput(pool, currentAmount);
                currentAmount = outputAmount;
                totalPriceImpact += priceImpact;
            }
            const slippageAdjusted = this.getSlippageAdjustedOutput({ ...route, outputAmount: currentAmount }, slippageBps);
            return {
                success: true,
                finalOutput: slippageAdjusted,
                actualPriceImpact: totalPriceImpact,
                gasEstimate: this.estimateGasForRoute(route)
            };
        }
        catch (error) {
            logger.error('Route simulation failed:', error);
            return {
                success: false,
                finalOutput: new anchor_1.BN(0),
                actualPriceImpact: 1,
                gasEstimate: 0
            };
        }
    }
    getPriceQuote(inputToken, outputToken, inputAmount) {
        const pools = this.getDirectPools(inputToken, outputToken);
        if (pools.length === 0)
            return null;
        let totalOutput = new anchor_1.BN(0);
        let totalLiquidity = new anchor_1.BN(0);
        for (const pool of pools) {
            const { outputAmount } = this.calculateSwapOutput(pool, inputAmount);
            totalOutput = totalOutput.add(outputAmount);
            totalLiquidity = totalLiquidity.add(pool.liquidity);
        }
        const price = totalOutput.mul(new anchor_1.BN(1e9)).div(inputAmount).div(new anchor_1.BN(pools.length)).toNumber() / 1e9;
        return {
            price,
            availableLiquidity: totalLiquidity,
            sources: pools.length
        };
    }
    async monitorPoolHealth(poolAddress) {
        const pool = this.poolCache.get(poolAddress.toBase58());
        if (!pool) {
            return {
                isHealthy: false,
                issues: ['Pool not found'],
                recommendations: ['Add pool to monitoring']
            };
        }
        const issues = [];
        const recommendations = [];
        // Check liquidity depth
        if (pool.liquidity.lt(new anchor_1.BN(this.MIN_LIQUIDITY_USD))) {
            issues.push('Low liquidity');
            recommendations.push('Wait for liquidity to increase before trading');
        }
        // Check price balance
        const priceRatio = pool.reserveA.mul(new anchor_1.BN(1e9)).div(pool.reserveB).toNumber() / 1e9;
        if (priceRatio > 100 || priceRatio < 0.01) {
            issues.push('Extreme price imbalance');
            recommendations.push('Avoid large trades in this pool');
        }
        // Check update freshness
        if (Date.now() - pool.lastUpdate > 10000) {
            issues.push('Stale pool data');
            recommendations.push('Refresh pool data before trading');
        }
        // Check historical metrics
        const historicalMetrics = await this.getHistoricalMetrics(poolAddress, 3600000);
        if (historicalMetrics && historicalMetrics.priceVolatility > 0.1) {
            issues.push('High price volatility');
            recommendations.push('Use tighter slippage protection');
        }
        return {
            isHealthy: issues.length === 0,
            issues,
            recommendations
        };
    }
    optimizeRouteForGas(route) {
        // Consolidate routes through the same pool
        const consolidatedSteps = [];
        const poolAmounts = new Map();
        for (const step of route.steps) {
            const poolKey = step.pool.address.toBase58();
            const existingAmount = poolAmounts.get(poolKey) || new anchor_1.BN(0);
            poolAmounts.set(poolKey, existingAmount.add(step.inputAmount));
        }
        // Rebuild route with consolidated amounts
        let remainingInput = route.inputAmount;
        let totalOutput = new anchor_1.BN(0);
        let totalFees = new anchor_1.BN(0);
        for (const [poolKey, amount] of poolAmounts) {
            if (remainingInput.isZero())
                break;
            const pool = this.poolCache.get(poolKey);
            if (!pool)
                continue;
            const inputForPool = anchor_1.BN.min(amount, remainingInput);
            const { outputAmount, priceImpact } = this.calculateSwapOutput(pool, inputForPool);
            const fee = inputForPool.mul(new anchor_1.BN(Math.floor(pool.fee * 1e9))).div(new anchor_1.BN(1e9));
            consolidatedSteps.push({
                pool,
                inputAmount: inputForPool,
                outputAmount,
                priceImpact,
                fee
            });
            remainingInput = remainingInput.sub(inputForPool);
            totalOutput = totalOutput.add(outputAmount);
            totalFees = totalFees.add(fee);
        }
        return {
            steps: consolidatedSteps,
            inputAmount: route.inputAmount,
            outputAmount: totalOutput,
            totalFees,
            priceImpact: consolidatedSteps.reduce((sum, step) => sum + step.priceImpact, 0),
            executionPrice: totalOutput.mul(new anchor_1.BN(1e9)).div(route.inputAmount).toNumber() / 1e9
        };
    }
    getPoolUtilization(poolAddress) {
        const pool = this.poolCache.get(poolAddress.toBase58());
        if (!pool || pool.liquidity.isZero())
            return 0;
        // Calculate 24h utilization rate
        const utilization = pool.volume24h.mul(new anchor_1.BN(100)).div(pool.liquidity).toNumber();
        return Math.min(utilization, 100);
    }
    async prefetchPoolsForTokens(tokens) {
        const poolsToFetch = new Set();
        for (let i = 0; i < tokens.length; i++) {
            for (let j = 0; j < tokens.length; j++) {
                if (i === j)
                    continue;
                const pairKey = this.getPairKey(tokens[i], tokens[j]);
                const poolAddrs = this.tokenPairIndex.get(pairKey);
                if (poolAddrs) {
                    poolAddrs.forEach(addr => poolsToFetch.add(addr));
                }
            }
        }
        const addresses = Array.from(poolsToFetch).map(addr => new web3_js_1.PublicKey(addr));
        await this.batchRefreshPools(addresses);
    }
    exportMetrics() {
        const protocolStats = new Map();
        for (const [protocol, source] of this.sources) {
            if (source.isActive) {
                protocolStats.set(protocol, this.getProtocolStats(protocol));
            }
        }
        return {
            protocolStats,
            poolCount: this.poolCache.size,
            totalLiquidity: this.getTotalLiquidity().toString(),
            activeSubscriptions: this.subscriptions.size,
            cacheHitRate: 0.95, // Would calculate from actual metrics
            lastUpdateTime: Date.now()
        };
    }
}
exports.LiquiditySourceManager = LiquiditySourceManager;
exports.default = LiquiditySourceManager;
//# sourceMappingURL=LiquiditySourceManager.js.map