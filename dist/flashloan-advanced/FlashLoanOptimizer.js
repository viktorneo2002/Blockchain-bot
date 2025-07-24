"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.FlashLoanOptimizer = void 0;
const web3_js_1 = require("@solana/web3.js");
const spl_token_1 = require("@solana/spl-token");
const bn_js_1 = __importDefault(require("bn.js"));
const buffer_1 = require("buffer");
class FlashLoanOptimizer {
    constructor(connection, wallet, config) {
        this.isRunning = false;
        this.connection = connection;
        this.wallet = wallet;
        this.config = config;
        this.flashLoanProviders = new Map();
        this.lookupTableCache = new Map();
    }
    async initialize() {
        await this.loadFlashLoanProviders();
        await this.loadLookupTables();
    }
    async loadFlashLoanProviders() {
        const solendProgramId = new web3_js_1.PublicKey('So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo');
        const portProgramId = new web3_js_1.PublicKey('Port7uDYB3wk6GJAw4KT1WpTeMtSu9bTcChBHkX2LfR');
        this.flashLoanProviders.set('solend', {
            programId: solendProgramId,
            poolAddress: new web3_js_1.PublicKey('8PbodeaosQP19SjYFx855UMqWxH2HynZLdBXmsrbac36'),
            feeAmount: new bn_js_1.default(9),
            maxAmount: new bn_js_1.default(1000000000000)
        });
        this.flashLoanProviders.set('port', {
            programId: portProgramId,
            poolAddress: new web3_js_1.PublicKey('6W6yiHr4oM6V8Z8f5X3qhQxV4b3qjB1xvUH2Ufn7Bwpi'),
            feeAmount: new bn_js_1.default(5),
            maxAmount: new bn_js_1.default(500000000000)
        });
    }
    async loadLookupTables() {
        const lookupTableAddresses = [
            'F14Cp89oAXMrNnaC4mKMNKHPWw2p2R4DRFAZEdJhUBkD',
            '2immgwYNHBbyVQKVGCEkgWpi53bLwWNRMB5G2nbgYV17',
            'D2aip5vULevznTC1xRuvQxEbUFBpfx84xCARvPXL5VaT'
        ];
        for (const address of lookupTableAddresses) {
            try {
                const lookupTableAccount = await this.connection.getAddressLookupTable(new web3_js_1.PublicKey(address));
                if (lookupTableAccount.value) {
                    this.lookupTableCache.set(address, lookupTableAccount.value);
                }
            }
            catch (error) {
                console.error(`Failed to load lookup table ${address}:`, error);
            }
        }
    }
    async findArbitrageOpportunities(tokenMints) {
        const opportunities = [];
        for (const inputMint of tokenMints) {
            for (const outputMint of tokenMints) {
                if (inputMint.equals(outputMint))
                    continue;
                const routes = await this.findOptimalRoutes(inputMint, outputMint);
                for (const route of routes) {
                    const profit = await this.calculateProfit(route);
                    if (profit.gt(new bn_js_1.default(0)) && this.isProfitable(profit, route.inputAmount)) {
                        opportunities.push({
                            inputMint,
                            outputMint,
                            inputAmount: route.inputAmount,
                            expectedOutput: route.expectedOutput,
                            profit,
                            route: route.swapLegs,
                            flashLoanProvider: route.flashLoanProvider
                        });
                    }
                }
            }
        }
        return opportunities.sort((a, b) => b.profit.cmp(a.profit));
    }
    async findOptimalRoutes(inputMint, outputMint) {
        const routes = [];
        const amounts = [
            new bn_js_1.default(100000000),
            new bn_js_1.default(1000000000),
            new bn_js_1.default(10000000000)
        ];
        for (const amount of amounts) {
            for (const [name, provider] of this.flashLoanProviders) {
                if (amount.lte(provider.maxAmount)) {
                    const directRoute = await this.findDirectRoute(inputMint, outputMint, amount);
                    if (directRoute) {
                        routes.push({
                            inputAmount: amount,
                            expectedOutput: directRoute.expectedOutput,
                            swapLegs: directRoute.legs,
                            flashLoanProvider: provider
                        });
                    }
                    const multiHopRoute = await this.findMultiHopRoute(inputMint, outputMint, amount);
                    if (multiHopRoute) {
                        routes.push({
                            inputAmount: amount,
                            expectedOutput: multiHopRoute.expectedOutput,
                            swapLegs: multiHopRoute.legs,
                            flashLoanProvider: provider
                        });
                    }
                }
            }
        }
        return routes;
    }
    async findDirectRoute(inputMint, outputMint, amount) {
        const dexes = ['raydium', 'orca', 'serum'];
        let bestRoute = null;
        let bestOutput = new bn_js_1.default(0);
        for (const dex of dexes) {
            try {
                const quote = await this.getQuote(dex, inputMint, outputMint, amount);
                if (quote && quote.outputAmount.gt(bestOutput)) {
                    bestOutput = quote.outputAmount;
                    bestRoute = {
                        expectedOutput: quote.outputAmount,
                        legs: [{
                                protocol: dex,
                                programId: quote.programId,
                                poolAddress: quote.poolAddress,
                                inputMint,
                                outputMint,
                                inputAmount: amount,
                                minOutputAmount: quote.outputAmount.muln(97).divn(100),
                                accounts: quote.accounts
                            }]
                    };
                }
            }
            catch (error) {
                continue;
            }
        }
        return bestRoute;
    }
    async findMultiHopRoute(inputMint, outputMint, amount) {
        const intermediateMints = [
            new web3_js_1.PublicKey('So11111111111111111111111111111111111111112'),
            new web3_js_1.PublicKey('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'),
            new web3_js_1.PublicKey('Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB')
        ];
        let bestRoute = null;
        let bestOutput = new bn_js_1.default(0);
        for (const intermediateMint of intermediateMints) {
            if (intermediateMint.equals(inputMint) || intermediateMint.equals(outputMint)) {
                continue;
            }
            const firstLeg = await this.findDirectRoute(inputMint, intermediateMint, amount);
            if (!firstLeg)
                continue;
            const secondLeg = await this.findDirectRoute(intermediateMint, outputMint, firstLeg.expectedOutput);
            if (secondLeg && secondLeg.expectedOutput.gt(bestOutput)) {
                bestOutput = secondLeg.expectedOutput;
                bestRoute = {
                    expectedOutput: secondLeg.expectedOutput,
                    legs: [...firstLeg.legs, ...secondLeg.legs]
                };
            }
        }
        return bestRoute;
    }
    async getQuote(dex, inputMint, outputMint, amount) {
        switch (dex) {
            case 'raydium':
                return this.getRaydiumQuote(inputMint, outputMint, amount);
            case 'orca':
                return this.getOrcaQuote(inputMint, outputMint, amount);
            case 'serum':
                return this.getSerumQuote(inputMint, outputMint, amount);
            default:
                return null;
        }
    }
    async getRaydiumQuote(inputMint, outputMint, amount) {
        const programId = new web3_js_1.PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
        const poolData = await this.findRaydiumPool(inputMint, outputMint);
        if (!poolData)
            return null;
        const outputAmount = this.calculateSwapOutput(amount, poolData.inputReserve, poolData.outputReserve, poolData.fee);
        return {
            programId,
            poolAddress: poolData.poolAddress,
            outputAmount,
            accounts: poolData.accounts
        };
    }
    async getOrcaQuote(inputMint, outputMint, amount) {
        const programId = new web3_js_1.PublicKey('9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP');
        const poolData = await this.findOrcaPool(inputMint, outputMint);
        if (!poolData)
            return null;
        const outputAmount = this.calculateSwapOutput(amount, poolData.inputReserve, poolData.outputReserve, poolData.fee);
        return {
            programId,
            poolAddress: poolData.poolAddress,
            outputAmount,
            accounts: poolData.accounts
        };
    }
    async getSerumQuote(inputMint, outputMint, amount) {
        const programId = new web3_js_1.PublicKey('9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin');
        const marketData = await this.findSerumMarket(inputMint, outputMint);
        if (!marketData)
            return null;
        const outputAmount = await this.getSerumMarketPrice(marketData.marketAddress, amount, marketData.isBid);
        return {
            programId,
            poolAddress: marketData.marketAddress,
            outputAmount,
            accounts: marketData.accounts
        };
    }
    calculateSwapOutput(inputAmount, inputReserve, outputReserve, feeBps) {
        const inputAmountWithFee = inputAmount.muln(10000 - feeBps);
        const numerator = inputAmountWithFee.mul(outputReserve);
        const denominator = inputReserve.muln(10000).add(inputAmountWithFee);
        return numerator.div(denominator);
    }
    async calculateProfit(route) {
        const flashLoanFee = route.flashLoanProvider.feeAmount
            .mul(route.inputAmount)
            .divn(10000);
        const totalCost = route.inputAmount.add(flashLoanFee);
        return route.expectedOutput.sub(totalCost);
    }
    isProfitable(profit, inputAmount) {
        const profitBps = profit.muln(10000).div(inputAmount).toNumber();
        return profitBps >= this.config.minProfitBps;
    }
    async executeArbitrage(route) {
        try {
            const transaction = await this.buildArbitrageTransaction(route);
            const signature = await this.sendOptimizedTransaction(transaction);
            return signature;
        }
        catch (error) {
            console.error('Arbitrage execution failed:', error);
            return null;
        }
    }
    async buildArbitrageTransaction(route) {
        const transaction = new web3_js_1.Transaction();
        // Add compute budget instructions
        transaction.add(web3_js_1.ComputeBudgetProgram.setComputeUnitLimit({
            units: this.config.computeUnitLimit
        }));
        transaction.add(web3_js_1.ComputeBudgetProgram.setComputeUnitPrice({
            microLamports: this.config.priorityFeePerCU
        }));
        // Add flash loan borrow instruction
        const borrowIx = await this.createFlashLoanBorrowInstruction(route.flashLoanProvider, route.inputMint, route.inputAmount);
        transaction.add(borrowIx);
        // Add swap instructions
        for (const leg of route.route) {
            const swapIx = await this.createSwapInstruction(leg);
            transaction.add(swapIx);
        }
        // Add flash loan repay instruction
        const repayAmount = route.inputAmount.add(route.flashLoanProvider.feeAmount.mul(route.inputAmount).divn(10000));
        const repayIx = await this.createFlashLoanRepayInstruction(route.flashLoanProvider, route.inputMint, repayAmount);
        transaction.add(repayIx);
        return transaction;
    }
    async createFlashLoanBorrowInstruction(provider, mint, amount) {
        const userTokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(mint, this.wallet.publicKey);
        const data = buffer_1.Buffer.alloc(17);
        data.writeUInt8(0, 0); // Borrow instruction
        data.writeBigUInt64LE(BigInt(amount.toString()), 1);
        return new web3_js_1.TransactionInstruction({
            keys: [
                { pubkey: provider.poolAddress, isSigner: false, isWritable: true },
                { pubkey: userTokenAccount, isSigner: false, isWritable: true },
                { pubkey: this.wallet.publicKey, isSigner: true, isWritable: false },
                { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false }
            ],
            programId: provider.programId,
            data
        });
    }
    async createFlashLoanRepayInstruction(provider, mint, amount) {
        const userTokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(mint, this.wallet.publicKey);
        const data = buffer_1.Buffer.alloc(17);
        data.writeUInt8(1, 0); // Repay instruction
        data.writeBigUInt64LE(BigInt(amount.toString()), 1);
        return new web3_js_1.TransactionInstruction({
            keys: [
                { pubkey: provider.poolAddress, isSigner: false, isWritable: true },
                { pubkey: userTokenAccount, isSigner: false, isWritable: true },
                { pubkey: this.wallet.publicKey, isSigner: true, isWritable: false },
                { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false }
            ],
            programId: provider.programId,
            data
        });
    }
    async createSwapInstruction(leg) {
        switch (leg.protocol) {
            case 'raydium':
                return this.createRaydiumSwapInstruction(leg);
            case 'orca':
                return this.createOrcaSwapInstruction(leg);
            case 'serum':
                return this.createSerumSwapInstruction(leg);
            default:
                throw new Error(`Unsupported protocol: ${leg.protocol}`);
        }
    }
    async createRaydiumSwapInstruction(leg) {
        const data = buffer_1.Buffer.alloc(16);
        data.writeBigUInt64LE(BigInt(leg.inputAmount.toString()), 0);
        data.writeBigUInt64LE(BigInt(leg.minOutputAmount.toString()), 8);
        return new web3_js_1.TransactionInstruction({
            keys: leg.accounts.map((account, index) => ({
                pubkey: account,
                isSigner: index === 0,
                isWritable: index < 4
            })),
            programId: leg.programId,
            data
        });
    }
    async createOrcaSwapInstruction(leg) {
        const data = buffer_1.Buffer.alloc(16);
        data.writeBigUInt64LE(BigInt(leg.inputAmount.toString()), 0);
        data.writeBigUInt64LE(BigInt(leg.minOutputAmount.toString()), 8);
        return new web3_js_1.TransactionInstruction({
            keys: leg.accounts.map((account, index) => ({
                pubkey: account,
                isSigner: index === 0,
                isWritable: index < 4
            })),
            programId: leg.programId,
            data
        });
    }
    async createSerumSwapInstruction(leg) {
        const data = buffer_1.Buffer.alloc(41);
        data.writeUInt8(0, 0); // New order v3
        data.writeBigUInt64LE(BigInt(leg.inputAmount.toString()), 1);
        data.writeBigUInt64LE(BigInt(leg.minOutputAmount.toString()), 9);
        data.writeUInt8(0, 17); // Side: bid
        data.writeUInt8(0, 18); // Order type: limit
        return new web3_js_1.TransactionInstruction({
            keys: leg.accounts.map((account, index) => ({
                pubkey: account,
                isSigner: index === 0,
                isWritable: index < 8
            })),
            programId: leg.programId,
            data
        });
    }
    async sendOptimizedTransaction(transaction) {
        transaction.recentBlockhash = (await this.connection.getLatestBlockhash('confirmed')).blockhash;
        transaction.feePayer = this.wallet.publicKey;
        // Create versioned transaction with lookup tables
        const lookupTables = Array.from(this.lookupTableCache.values());
        const message = new web3_js_1.TransactionMessage({
            payerKey: this.wallet.publicKey,
            recentBlockhash: transaction.recentBlockhash,
            instructions: transaction.instructions
        });
        const versionedMessage = message.compileToV0Message(lookupTables);
        const versionedTx = new web3_js_1.VersionedTransaction(versionedMessage);
        versionedTx.sign([this.wallet]);
        let retries = 0;
        while (retries < this.config.maxRetries) {
            try {
                const signature = await this.connection.sendTransaction(versionedTx, {
                    skipPreflight: true,
                    maxRetries: 0,
                    preflightCommitment: 'processed'
                });
                const confirmation = await this.confirmTransactionWithTimeout(signature, this.config.confirmationTimeout);
                if (confirmation) {
                    return signature;
                }
            }
            catch (error) {
                console.error(`Transaction attempt ${retries + 1} failed:`, error);
                retries++;
                if (retries < this.config.maxRetries) {
                    await new Promise(resolve => setTimeout(resolve, 100 * retries));
                }
            }
        }
        throw new Error('Transaction failed after max retries');
    }
    async confirmTransactionWithTimeout(signature, timeout) {
        const startTime = Date.now();
        while (Date.now() - startTime < timeout) {
            const status = await this.connection.getSignatureStatus(signature);
            if (status.value?.confirmationStatus === 'confirmed' ||
                status.value?.confirmationStatus === 'finalized') {
                return true;
            }
            if (status.value?.err) {
                throw new Error(`Transaction failed: ${JSON.stringify(status.value.err)}`);
            }
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        return false;
    }
    async findRaydiumPool(inputMint, outputMint) {
        const pools = await this.getRaydiumPools();
        for (const pool of pools) {
            if ((pool.baseMint.equals(inputMint) && pool.quoteMint.equals(outputMint)) ||
                (pool.baseMint.equals(outputMint) && pool.quoteMint.equals(inputMint))) {
                const poolAccount = await this.connection.getAccountInfo(pool.address);
                if (!poolAccount)
                    continue;
                const poolData = this.parseRaydiumPoolData(poolAccount.data);
                return {
                    poolAddress: pool.address,
                    inputReserve: pool.baseMint.equals(inputMint) ?
                        poolData.baseReserve : poolData.quoteReserve,
                    outputReserve: pool.baseMint.equals(inputMint) ?
                        poolData.quoteReserve : poolData.baseReserve,
                    fee: 25, // 0.25%
                    accounts: [
                        this.wallet.publicKey,
                        pool.address,
                        pool.authority,
                        pool.openOrders,
                        pool.targetOrders,
                        pool.baseVault,
                        pool.quoteVault,
                        pool.marketProgramId,
                        pool.marketId,
                        pool.marketBids,
                        pool.marketAsks,
                        pool.marketEventQueue,
                        pool.marketBaseVault,
                        pool.marketQuoteVault,
                        pool.marketAuthority,
                        spl_token_1.TOKEN_PROGRAM_ID
                    ]
                };
            }
        }
        return null;
    }
    async findOrcaPool(inputMint, outputMint) {
        const pools = await this.getOrcaPools();
        for (const pool of pools) {
            if ((pool.tokenMintA.equals(inputMint) && pool.tokenMintB.equals(outputMint)) ||
                (pool.tokenMintA.equals(outputMint) && pool.tokenMintB.equals(inputMint))) {
                const poolAccount = await this.connection.getAccountInfo(pool.address);
                if (!poolAccount)
                    continue;
                const poolData = this.parseOrcaPoolData(poolAccount.data);
                return {
                    poolAddress: pool.address,
                    inputReserve: pool.tokenMintA.equals(inputMint) ?
                        poolData.tokenAAmount : poolData.tokenBAmount,
                    outputReserve: pool.tokenMintA.equals(inputMint) ?
                        poolData.tokenBAmount : poolData.tokenAAmount,
                    fee: poolData.fee,
                    accounts: [
                        this.wallet.publicKey,
                        pool.address,
                        pool.authority,
                        await (0, spl_token_1.getAssociatedTokenAddress)(inputMint, this.wallet.publicKey),
                        pool.tokenVaultA,
                        pool.tokenVaultB,
                        await (0, spl_token_1.getAssociatedTokenAddress)(outputMint, this.wallet.publicKey),
                        pool.poolMint,
                        pool.feeAccount,
                        spl_token_1.TOKEN_PROGRAM_ID
                    ]
                };
            }
        }
        return null;
    }
    async findSerumMarket(inputMint, outputMint) {
        const markets = await this.getSerumMarkets();
        for (const market of markets) {
            if ((market.baseMint.equals(inputMint) && market.quoteMint.equals(outputMint)) ||
                (market.baseMint.equals(outputMint) && market.quoteMint.equals(inputMint))) {
                return {
                    marketAddress: market.address,
                    isBid: market.baseMint.equals(outputMint),
                    accounts: [
                        market.address,
                        market.requestQueue,
                        market.eventQueue,
                        market.bids,
                        market.asks,
                        market.baseVault,
                        market.quoteVault,
                        market.baseMint,
                        market.quoteMint,
                        await (0, spl_token_1.getAssociatedTokenAddress)(inputMint, this.wallet.publicKey),
                        await (0, spl_token_1.getAssociatedTokenAddress)(outputMint, this.wallet.publicKey),
                        this.wallet.publicKey,
                        market.vaultSigner,
                        spl_token_1.TOKEN_PROGRAM_ID
                    ]
                };
            }
        }
        return null;
    }
    async getRaydiumPools() {
        const programId = new web3_js_1.PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
        const poolLayout = buffer_1.Buffer.from([
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        ]);
        const accounts = await this.connection.getProgramAccounts(programId, {
            filters: [
                { dataSize: 752 },
                { memcmp: { offset: 0, bytes: poolLayout.toString('base64') } }
            ]
        });
        const pools = [];
        for (const account of accounts) {
            const data = account.account.data;
            const pool = {
                address: account.pubkey,
                status: data.readUInt8(0),
                nonce: data.readUInt8(1),
                orderNum: data.readBigUInt64LE(2),
                depth: data.readBigUInt64LE(10),
                coinDecimals: data.readUInt8(18),
                pcDecimals: data.readUInt8(19),
                state: data.readUInt8(20),
                resetFlag: data.readUInt8(21),
                minSize: data.readBigUInt64LE(22),
                volMaxCutRatio: data.readUInt16LE(30),
                amountWaveRatio: data.readUInt16LE(32),
                baseMint: new web3_js_1.PublicKey(data.slice(64, 96)),
                quoteMint: new web3_js_1.PublicKey(data.slice(96, 128)),
                baseVault: new web3_js_1.PublicKey(data.slice(128, 160)),
                quoteVault: new web3_js_1.PublicKey(data.slice(160, 192)),
                marketId: new web3_js_1.PublicKey(data.slice(256, 288)),
                marketProgramId: new web3_js_1.PublicKey(data.slice(288, 320)),
                authority: new web3_js_1.PublicKey(data.slice(320, 352)),
                openOrders: new web3_js_1.PublicKey(data.slice(352, 384)),
                targetOrders: new web3_js_1.PublicKey(data.slice(384, 416)),
                marketBids: new web3_js_1.PublicKey(data.slice(416, 448)),
                marketAsks: new web3_js_1.PublicKey(data.slice(448, 480)),
                marketEventQueue: new web3_js_1.PublicKey(data.slice(480, 512)),
                marketBaseVault: new web3_js_1.PublicKey(data.slice(512, 544)),
                marketQuoteVault: new web3_js_1.PublicKey(data.slice(544, 576)),
                marketAuthority: new web3_js_1.PublicKey(data.slice(576, 608))
            };
            if (pool.status === 6) {
                pools.push(pool);
            }
        }
        return pools;
    }
    async getOrcaPools() {
        const whirlpoolProgramId = new web3_js_1.PublicKey('whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc');
        const configPubkey = new web3_js_1.PublicKey('2LecshUwdy9xi7meFgHtFJQNSKk4KdTrcpvaB56dP2NQ');
        const configAccount = await this.connection.getAccountInfo(configPubkey);
        if (!configAccount)
            return [];
        const pools = [];
        const whirlpoolAccounts = await this.connection.getProgramAccounts(whirlpoolProgramId, {
            filters: [{ dataSize: 653 }]
        });
        for (const account of whirlpoolAccounts) {
            const data = account.account.data;
            const discriminator = data.slice(0, 8);
            if (discriminator.toString('hex') === 'f8c69e91e1648700') {
                const pool = {
                    address: account.pubkey,
                    whirlpoolsConfig: new web3_js_1.PublicKey(data.slice(8, 40)),
                    whirlpoolBump: data[40],
                    tickSpacing: data.readUInt16LE(41),
                    tickSpacingSeed: data.slice(43, 45),
                    feeRate: data.readUInt16LE(45),
                    protocolFeeRate: data.readUInt16LE(47),
                    liquidity: new bn_js_1.default(data.slice(49, 65), 'le'),
                    sqrtPrice: new bn_js_1.default(data.slice(65, 81), 'le'),
                    tickCurrentIndex: data.readInt32LE(81),
                    protocolFeeOwedA: new bn_js_1.default(data.slice(85, 93), 'le'),
                    protocolFeeOwedB: new bn_js_1.default(data.slice(93, 101), 'le'),
                    tokenMintA: new web3_js_1.PublicKey(data.slice(101, 133)),
                    tokenVaultA: new web3_js_1.PublicKey(data.slice(133, 165)),
                    feeGrowthGlobalA: new bn_js_1.default(data.slice(165, 181), 'le'),
                    tokenMintB: new web3_js_1.PublicKey(data.slice(181, 213)),
                    tokenVaultB: new web3_js_1.PublicKey(data.slice(213, 245)),
                    feeGrowthGlobalB: new bn_js_1.default(data.slice(245, 261), 'le'),
                    rewardLastUpdatedTimestamp: new bn_js_1.default(data.slice(261, 269), 'le'),
                    authority: web3_js_1.PublicKey.findProgramAddressSync([buffer_1.Buffer.from('whirlpool'), account.pubkey.toBuffer()], whirlpoolProgramId)[0],
                    poolMint: new web3_js_1.PublicKey(data.slice(269, 301)),
                    feeAccount: new web3_js_1.PublicKey(data.slice(301, 333))
                };
                if (pool.liquidity.gt(new bn_js_1.default(0))) {
                    pools.push(pool);
                }
            }
        }
        return pools;
    }
    async getSerumMarkets() {
        const serumProgramId = new web3_js_1.PublicKey('9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin');
        const markets = [];
        const marketAddresses = [
            'HWHvQhFmJB3NUcu1aihKmrKegfVxBEHzwVX6yZCKEsi1',
            'DESVgJVGajEgKGXhb6XmqDHGz3VjdgP7rEVESBgxmroY',
            '9wFFyRfZBsuAha4YcuxcXLKwMxJR43S7fPfQLusDBzvT',
            '5cLrMai1DsLRYc1Nio9qMTicsWtvzjzZfJPXyAoF4t1Z'
        ];
        for (const address of marketAddresses) {
            try {
                const marketPubkey = new web3_js_1.PublicKey(address);
                const accountInfo = await this.connection.getAccountInfo(marketPubkey);
                if (accountInfo && accountInfo.data.length >= 388) {
                    const data = accountInfo.data;
                    const market = {
                        address: marketPubkey,
                        accountFlags: new bn_js_1.default(data.slice(0, 8), 'le'),
                        ownAddress: new web3_js_1.PublicKey(data.slice(13, 45)),
                        vaultSignerNonce: new bn_js_1.default(data.slice(45, 53), 'le'),
                        baseMint: new web3_js_1.PublicKey(data.slice(53, 85)),
                        quoteMint: new web3_js_1.PublicKey(data.slice(85, 117)),
                        baseVault: new web3_js_1.PublicKey(data.slice(117, 149)),
                        baseDepositsTotal: new bn_js_1.default(data.slice(149, 157), 'le'),
                        baseFeesAccrued: new bn_js_1.default(data.slice(157, 165), 'le'),
                        quoteVault: new web3_js_1.PublicKey(data.slice(165, 197)),
                        quoteDepositsTotal: new bn_js_1.default(data.slice(197, 205), 'le'),
                        quoteFeesAccrued: new bn_js_1.default(data.slice(205, 213), 'le'),
                        quoteDustThreshold: new bn_js_1.default(data.slice(213, 221), 'le'),
                        requestQueue: new web3_js_1.PublicKey(data.slice(221, 253)),
                        eventQueue: new web3_js_1.PublicKey(data.slice(253, 285)),
                        bids: new web3_js_1.PublicKey(data.slice(285, 317)),
                        asks: new web3_js_1.PublicKey(data.slice(317, 349)),
                        baseLotSize: new bn_js_1.default(data.slice(349, 357), 'le'),
                        quoteLotSize: new bn_js_1.default(data.slice(357, 365), 'le'),
                        feeRateBps: new bn_js_1.default(data.slice(365, 373), 'le'),
                        vaultSigner: web3_js_1.PublicKey.createProgramAddressSync([marketPubkey.toBuffer(), data.slice(45, 53)], serumProgramId)
                    };
                    if (market.baseDepositsTotal.gt(new bn_js_1.default(0)) &&
                        market.quoteDepositsTotal.gt(new bn_js_1.default(0))) {
                        markets.push(market);
                    }
                }
            }
            catch (error) {
                console.error(`Failed to load market ${address}:`, error);
            }
        }
        return markets;
    }
    parseRaydiumPoolData(data) {
        return {
            baseReserve: new bn_js_1.default(data.slice(64, 72), 'le'),
            quoteReserve: new bn_js_1.default(data.slice(72, 80), 'le')
        };
    }
    parseOrcaPoolData(data) {
        return {
            tokenAAmount: new bn_js_1.default(data.slice(72, 80), 'le'),
            tokenBAmount: new bn_js_1.default(data.slice(80, 88), 'le'),
            fee: data.readUInt16LE(108)
        };
    }
    async getSerumMarketPrice(marketAddress, amount, isBid) {
        const marketAccount = await this.connection.getAccountInfo(marketAddress);
        if (!marketAccount)
            throw new Error('Market not found');
        const marketData = marketAccount.data;
        const bidsAddress = new web3_js_1.PublicKey(marketData.slice(285, 317));
        const asksAddress = new web3_js_1.PublicKey(marketData.slice(317, 349));
        const baseLotSize = new bn_js_1.default(marketData.slice(349, 357), 'le');
        const quoteLotSize = new bn_js_1.default(marketData.slice(357, 365), 'le');
        const orderbookAddress = isBid ? bidsAddress : asksAddress;
        const orderbook = await this.connection.getAccountInfo(orderbookAddress);
        if (!orderbook)
            throw new Error('Orderbook not found');
        const slab = this.parseSerumSlab(orderbook.data);
        let totalBase = new bn_js_1.default(0);
        let totalQuote = new bn_js_1.default(0);
        let remainingAmount = amount;
        for (const order of slab) {
            const orderSize = order.quantity.mul(baseLotSize);
            const orderPrice = order.price.mul(quoteLotSize).div(baseLotSize);
            if (remainingAmount.lte(orderSize)) {
                totalBase = totalBase.add(remainingAmount);
                totalQuote = totalQuote.add(remainingAmount.mul(orderPrice).div(baseLotSize));
                break;
            }
            else {
                totalBase = totalBase.add(orderSize);
                totalQuote = totalQuote.add(orderSize.mul(orderPrice).div(baseLotSize));
                remainingAmount = remainingAmount.sub(orderSize);
            }
        }
        return isBid ? totalBase : totalQuote;
    }
    parseSerumSlab(data) {
        const orders = [];
        const headerLen = 13;
        const bumpIndex = data.readUInt32LE(0);
        const numEntries = data.readUInt32LE(4);
        for (let i = 0; i < numEntries; i++) {
            const offset = headerLen + i * 80;
            if (offset + 80 > data.length)
                break;
            const tag = data.readUInt8(offset);
            if (tag === 1) { // Inner node
                orders.push({
                    price: new bn_js_1.default(data.slice(offset + 1, offset + 9), 'le'),
                    quantity: new bn_js_1.default(data.slice(offset + 9, offset + 17), 'le'),
                    owner: new web3_js_1.PublicKey(data.slice(offset + 17, offset + 49))
                });
            }
        }
        return orders.sort((a, b) => a.price.cmp(b.price));
    }
    async start() {
        this.isRunning = true;
        console.log('FlashLoanOptimizer started');
        const tokenMints = [
            new web3_js_1.PublicKey('So11111111111111111111111111111111111111112'),
            new web3_js_1.PublicKey('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'),
            new web3_js_1.PublicKey('Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'),
            new web3_js_1.PublicKey('7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs'),
            new web3_js_1.PublicKey('mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So')
        ];
        while (this.isRunning) {
            try {
                const opportunities = await this.findArbitrageOpportunities(tokenMints);
                for (const opportunity of opportunities.slice(0, 3)) {
                    if (opportunity.profit.gt(new bn_js_1.default(0))) {
                        console.log(`Arbitrage opportunity found: ${opportunity.profit.toString()}`);
                        const signature = await this.executeArbitrage(opportunity);
                        if (signature) {
                            console.log(`Arbitrage executed: ${signature}`);
                        }
                    }
                }
                await new Promise(resolve => setTimeout(resolve, 500));
            }
            catch (error) {
                console.error('Error in main loop:', error);
                await new Promise(resolve => setTimeout(resolve, 5000));
            }
        }
    }
    async stop() {
        this.isRunning = false;
        console.log('FlashLoanOptimizer stopped');
    }
    async getBalance(mint) {
        try {
            const tokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(mint, this.wallet.publicKey);
            const account = await (0, spl_token_1.getAccount)(this.connection, tokenAccount);
            return new bn_js_1.default(account.amount.toString());
        }
        catch (error) {
            return new bn_js_1.default(0);
        }
    }
    async monitorTransaction(signature) {
        const startTime = Date.now();
        const timeout = 30000;
        while (Date.now() - startTime < timeout) {
            const status = await this.connection.getSignatureStatus(signature);
            if (status.value?.confirmationStatus === 'finalized') {
                const tx = await this.connection.getTransaction(signature, {
                    maxSupportedTransactionVersion: 0
                });
                if (tx?.meta?.err) {
                    console.error('Transaction failed:', tx.meta.err);
                    return false;
                }
                return true;
            }
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
        return false;
    }
    validateConfig() {
        if (this.config.minProfitBps < 1) {
            console.error('Min profit BPS must be at least 1');
            return false;
        }
        if (this.config.maxSlippageBps > 100) {
            console.error('Max slippage BPS cannot exceed 100');
            return false;
        }
        if (this.config.computeUnitLimit < 200000) {
            console.error('Compute unit limit too low');
            return false;
        }
        return true;
    }
    async getOptimalLoanAmount(mint, route) {
        const maxAmount = this.config.maxLoanAmount;
        const testAmounts = [
            maxAmount.divn(100),
            maxAmount.divn(10),
            maxAmount.divn(4),
            maxAmount.divn(2),
            maxAmount
        ];
        let optimalAmount = new bn_js_1.default(0);
        let maxProfit = new bn_js_1.default(0);
        for (const amount of testAmounts) {
            try {
                const simulatedRoute = await this.simulateRoute(route, amount);
                const profit = simulatedRoute.expectedOutput.sub(amount);
                if (profit.gt(maxProfit)) {
                    maxProfit = profit;
                    optimalAmount = amount;
                }
            }
            catch (error) {
                continue;
            }
        }
        return optimalAmount;
    }
    async simulateRoute(route, inputAmount) {
        let currentAmount = inputAmount;
        for (const leg of route) {
            const output = await this.simulateSwap(leg.protocol, leg.poolAddress, leg.inputMint, leg.outputMint, currentAmount);
            currentAmount = output;
        }
        return { expectedOutput: currentAmount };
    }
    async simulateSwap(protocol, poolAddress, inputMint, outputMint, amount) {
        const poolAccount = await this.connection.getAccountInfo(poolAddress);
        if (!poolAccount)
            throw new Error('Pool not found');
        switch (protocol) {
            case 'raydium': {
                const poolData = this.parseRaydiumPoolData(poolAccount.data);
                return this.calculateSwapOutput(amount, poolData.baseReserve, poolData.quoteReserve, 25);
            }
            case 'orca': {
                const poolData = this.parseOrcaPoolData(poolAccount.data);
                return this.calculateSwapOutput(amount, poolData.tokenAAmount, poolData.tokenBAmount, poolData.fee);
            }
            case 'serum': {
                return this.getSerumMarketPrice(poolAddress, amount, false);
            }
            default:
                throw new Error(`Unknown protocol: ${protocol}`);
        }
    }
    async emergencyWithdraw(mint) {
        try {
            const tokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(mint, this.wallet.publicKey);
            const balance = await this.getBalance(mint);
            if (balance.eq(new bn_js_1.default(0)))
                return;
            const transaction = new web3_js_1.Transaction().add((0, spl_token_1.createTransferInstruction)(tokenAccount, tokenAccount, this.wallet.publicKey, BigInt(balance.toString())));
            const signature = await (0, web3_js_1.sendAndConfirmTransaction)(this.connection, transaction, [this.wallet], { commitment: 'confirmed' });
            console.log(`Emergency withdrawal completed: ${signature}`);
        }
        catch (error) {
            console.error('Emergency withdrawal failed:', error);
        }
    }
    getStatus() {
        return {
            isRunning: this.isRunning,
            flashLoanProviders: this.flashLoanProviders.size,
            lookupTables: this.lookupTableCache.size
        };
    }
    async updatePriorityFee() {
        try {
            const recentFees = await this.connection.getRecentPrioritizationFees();
            if (recentFees.length > 0) {
                const avgFee = recentFees.reduce((sum, fee) => sum + fee.prioritizationFee, 0) / recentFees.length;
                this.config.priorityFeePerCU = Math.max(Math.floor(avgFee * 1.2), 1000);
            }
        }
        catch (error) {
            console.error('Failed to update priority fee:', error);
        }
    }
    async checkHealthFactors() {
        const solBalance = await this.connection.getBalance(this.wallet.publicKey);
        if (solBalance < 50000000) {
            console.warn('Low SOL balance for fees');
            return false;
        }
        const blockHeight = await this.connection.getBlockHeight();
        const slot = await this.connection.getSlot();
        if (slot - blockHeight > 150) {
            console.warn('Network congestion detected');
            return false;
        }
        return true;
    }
    async optimizeTransaction(tx) {
        const simResult = await this.connection.simulateTransaction(tx);
        if (simResult.value.err) {
            throw new Error(`Simulation failed: ${JSON.stringify(simResult.value.err)}`);
        }
        const unitsConsumed = simResult.value.unitsConsumed || 200000;
        const optimizedUnits = Math.min(Math.ceil(unitsConsumed * 1.1), this.config.computeUnitLimit);
        tx.instructions[0] = web3_js_1.ComputeBudgetProgram.setComputeUnitLimit({
            units: optimizedUnits
        });
        return tx;
    }
    logArbitrageMetrics(route, success, signature) {
        const metrics = {
            timestamp: new Date().toISOString(),
            inputMint: route.inputMint.toString(),
            outputMint: route.outputMint.toString(),
            inputAmount: route.inputAmount.toString(),
            expectedProfit: route.profit.toString(),
            success,
            signature,
            routeLength: route.route.length,
            flashLoanProvider: route.flashLoanProvider.programId.toString()
        };
        console.log('Arbitrage metrics:', JSON.stringify(metrics));
    }
}
exports.FlashLoanOptimizer = FlashLoanOptimizer;
exports.default = FlashLoanOptimizer;
//# sourceMappingURL=FlashLoanOptimizer.js.map