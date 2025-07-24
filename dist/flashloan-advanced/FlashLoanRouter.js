"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MODULE_READY = exports.PERFORMANCE_CONFIG = exports.BUILD_DATE = exports.VERSION = exports.FlashLoanRouterFactory = exports.FlashLoanMonitor = exports.ValidationUtils = exports.FlashLoanError = exports.InsufficientLiquidityError = exports.MIN_PROFIT_THRESHOLD = exports.DEFAULT_PRIORITY_FEE = exports.DEFAULT_SLIPPAGE_BPS = exports.FlashLoanRouter = void 0;
exports.calculateOptimalLoanAmount = calculateOptimalLoanAmount;
exports.performSafetyChecks = performSafetyChecks;
exports.initialize = initialize;
exports.cleanup = cleanup;
exports.isFlashLoanError = isFlashLoanError;
exports.isInsufficientLiquidityError = isInsufficientLiquidityError;
const web3_js_1 = require("@solana/web3.js");
const spl_token_1 = require("@solana/spl-token");
const anchor_1 = require("@project-serum/anchor");
const bs58_1 = __importDefault(require("bs58"));
class FlashLoanRouter {
    constructor(connection, wallet) {
        this.MAX_RETRIES = 3;
        this.SIMULATION_COMMITMENT = 'processed';
        this.EXECUTION_COMMITMENT = 'confirmed';
        this.PRIORITY_FEE_PERCENTILE = 90;
        this.MAX_COMPUTE_UNITS = 1400000;
        this.JITO_TIP_ACCOUNTS = [
            '96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5',
            'HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe',
            'Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY',
            'ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49',
            'DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh',
            'ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt',
            'DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL',
            'HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe'
        ];
        this.connection = connection;
        this.wallet = wallet;
        this.providers = new Map();
        this.lookupTableCache = new Map();
        this.initializeProviders();
    }
    initializeProviders() {
        this.providers.set('solend', {
            name: 'Solend',
            programId: new web3_js_1.PublicKey('So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo'),
            loanAccount: new web3_js_1.PublicKey('DdZR6zRFiUt4S5mg7AV1uKB2z1f1WzcNYCaTEEWPAuby'),
            feeRate: 0.0009,
            maxLoanAmount: new anchor_1.BN(10000000000000),
            priority: 1
        });
        this.providers.set('port', {
            name: 'Port Finance',
            programId: new web3_js_1.PublicKey('Port7uDYB3wk5GJAw4c4QHXoPgaCQ3LST4gfgnM4n1w'),
            loanAccount: new web3_js_1.PublicKey('6W4W9zk6nxGLnGJrRKvMKTAeQcSfBNbux3N3CvPANNFp'),
            feeRate: 0.0008,
            maxLoanAmount: new anchor_1.BN(5000000000000),
            priority: 2
        });
        this.providers.set('kamino', {
            name: 'Kamino',
            programId: new web3_js_1.PublicKey('KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD'),
            loanAccount: new web3_js_1.PublicKey('7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF'),
            feeRate: 0.0007,
            maxLoanAmount: new anchor_1.BN(8000000000000),
            priority: 3
        });
    }
    async executeFlashLoan(params) {
        const startTime = Date.now();
        try {
            const route = await this.findOptimalRoute(params);
            if (!route || route.confidence < 0.85) {
                throw new Error(`No viable route found. Best confidence: ${route?.confidence || 0}`);
            }
            const transaction = await this.buildFlashLoanTransaction(route, params);
            const signature = await this.executeWithRetry(transaction);
            const executionTime = Date.now() - startTime;
            console.log(`Flash loan executed in ${executionTime}ms with provider ${route.provider.name}`);
            return signature;
        }
        catch (error) {
            console.error('Flash loan execution failed:', error);
            throw error;
        }
    }
    async findOptimalRoute(params) {
        const routes = await Promise.all(Array.from(this.providers.values())
            .filter(provider => provider.maxLoanAmount.gte(params.amount))
            .sort((a, b) => a.priority - b.priority)
            .map(provider => this.evaluateRoute(provider, params)));
        const viableRoutes = routes
            .filter(route => route !== null)
            .sort((a, b) => {
            const scoreA = this.calculateRouteScore(a);
            const scoreB = this.calculateRouteScore(b);
            return scoreB - scoreA;
        });
        return viableRoutes[0] || null;
    }
    calculateRouteScore(route) {
        const profitWeight = 0.4;
        const feeWeight = 0.3;
        const confidenceWeight = 0.3;
        const normalizedProfit = route.estimatedProfit.toNumber() / 1000000;
        const normalizedFee = 1 - (route.estimatedFee.toNumber() / 1000000);
        return (normalizedProfit * profitWeight +
            normalizedFee * feeWeight +
            route.confidence * confidenceWeight);
    }
    async evaluateRoute(provider, params) {
        try {
            const fee = params.amount.mul(new anchor_1.BN(provider.feeRate * 10000)).div(new anchor_1.BN(10000));
            const repaymentAmount = this.calculateRepaymentAmount(params.amount, fee, params.repaymentStrategy);
            const testTransaction = await this.buildTestTransaction(provider, params, repaymentAmount);
            const simulation = await this.simulateTransaction(testTransaction);
            if (!simulation || simulation.err) {
                return null;
            }
            const estimatedProfit = this.estimateProfit(simulation, params.amount, fee);
            const confidence = this.calculateConfidence(simulation, provider);
            return {
                provider,
                estimatedFee: fee,
                estimatedProfit,
                confidence,
                simulationResult: simulation
            };
        }
        catch (error) {
            console.warn(`Route evaluation failed for ${provider.name}:`, error);
            return null;
        }
    }
    calculateRepaymentAmount(principal, fee, strategy) {
        switch (strategy.type) {
            case 'FIXED':
                return principal.add(fee).add(new anchor_1.BN(strategy.value));
            case 'PERCENTAGE':
                const percentageFee = principal.mul(new anchor_1.BN(strategy.value * 100)).div(new anchor_1.BN(10000));
                return principal.add(fee).add(percentageFee);
            case 'PROFIT_SHARE':
                return principal.add(fee);
            default:
                return principal.add(fee);
        }
    }
    async buildFlashLoanTransaction(route, params) {
        const provider = route.provider;
        const fee = route.estimatedFee;
        const repaymentAmount = this.calculateRepaymentAmount(params.amount, fee, params.repaymentStrategy);
        const borrowerAta = await (0, spl_token_1.getAssociatedTokenAddress)(params.tokenMint, this.wallet.publicKey);
        const instructions = [];
        // Add compute budget instructions
        instructions.push(web3_js_1.ComputeBudgetProgram.setComputeUnitLimit({
            units: this.MAX_COMPUTE_UNITS
        }));
        const priorityFee = params.priorityFee || await this.getDynamicPriorityFee();
        instructions.push(web3_js_1.ComputeBudgetProgram.setComputeUnitPrice({
            microLamports: priorityFee
        }));
        // Create ATA if needed
        try {
            await (0, spl_token_1.getAccount)(this.connection, borrowerAta);
        }
        catch (error) {
            if (error instanceof spl_token_1.TokenAccountNotFoundError) {
                instructions.push((0, spl_token_1.createAssociatedTokenAccountInstruction)(this.wallet.publicKey, borrowerAta, this.wallet.publicKey, params.tokenMint));
            }
        }
        // Add flash loan borrow instruction
        instructions.push(await this.createBorrowInstruction(provider, params.amount, params.tokenMint, borrowerAta));
        // Add user's target instructions
        instructions.push(...params.targetInstructions);
        // Add flash loan repay instruction
        instructions.push(await this.createRepayInstruction(provider, repaymentAmount, params.tokenMint, borrowerAta));
        // Add Jito tip if profitable
        if (route.estimatedProfit.gt(new anchor_1.BN(1000000))) {
            const tipAmount = route.estimatedProfit.mul(new anchor_1.BN(100)).div(new anchor_1.BN(10000)); // 1% tip
            const tipAccount = this.getRandomJitoTipAccount();
            instructions.push(web3_js_1.SystemProgram.transfer({
                fromPubkey: this.wallet.publicKey,
                toPubkey: new web3_js_1.PublicKey(tipAccount),
                lamports: tipAmount.toNumber()
            }));
        }
        // Build versioned transaction with lookup tables
        const lookupTables = await this.fetchLookupTables(instructions);
        const blockhash = await this.connection.getLatestBlockhash(this.EXECUTION_COMMITMENT);
        const messageV0 = new web3_js_1.TransactionMessage({
            payerKey: this.wallet.publicKey,
            recentBlockhash: blockhash.blockhash,
            instructions
        }).compileToV0Message(lookupTables);
        const transaction = new web3_js_1.VersionedTransaction(messageV0);
        transaction.sign([this.wallet]);
        return transaction;
    }
    async createBorrowInstruction(provider, amount, tokenMint, borrowerAta) {
        const data = Buffer.concat([
            Buffer.from([0x01]), // Borrow instruction discriminator
            amount.toArrayLike(Buffer, 'le', 8)
        ]);
        const keys = [
            { pubkey: provider.loanAccount, isSigner: false, isWritable: true },
            { pubkey: borrowerAta, isSigner: false, isWritable: true },
            { pubkey: this.wallet.publicKey, isSigner: true, isWritable: false },
            { pubkey: tokenMint, isSigner: false, isWritable: false },
            { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false }
        ];
        return new web3_js_1.TransactionInstruction({
            keys,
            programId: provider.programId,
            data
        });
    }
    async createRepayInstruction(provider, amount, tokenMint, borrowerAta) {
        const data = Buffer.concat([
            Buffer.from([0x02]), // Repay instruction discriminator
            amount.toArrayLike(Buffer, 'le', 8)
        ]);
        const keys = [
            { pubkey: provider.loanAccount, isSigner: false, isWritable: true },
            { pubkey: borrowerAta, isSigner: false, isWritable: true },
            { pubkey: this.wallet.publicKey, isSigner: true, isWritable: false },
            { pubkey: tokenMint, isSigner: false, isWritable: false },
            { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false }
        ];
        return new web3_js_1.TransactionInstruction({
            keys,
            programId: provider.programId,
            data
        });
    }
    async buildTestTransaction(provider, params, repaymentAmount) {
        const borrowerAta = await (0, spl_token_1.getAssociatedTokenAddress)(params.tokenMint, this.wallet.publicKey);
        const instructions = [
            web3_js_1.ComputeBudgetProgram.setComputeUnitLimit({
                units: this.MAX_COMPUTE_UNITS
            }),
            await this.createBorrowInstruction(provider, params.amount, params.tokenMint, borrowerAta),
            ...params.targetInstructions,
            await this.createRepayInstruction(provider, repaymentAmount, params.tokenMint, borrowerAta)
        ];
        const blockhash = await this.connection.getLatestBlockhash();
        const messageV0 = new web3_js_1.TransactionMessage({
            payerKey: this.wallet.publicKey,
            recentBlockhash: blockhash.blockhash,
            instructions
        }).compileToV0Message();
        const transaction = new web3_js_1.VersionedTransaction(messageV0);
        transaction.sign([this.wallet]);
        return transaction;
    }
    async simulateTransaction(transaction) {
        try {
            const simulation = await this.connection.simulateTransaction(transaction, {
                commitment: this.SIMULATION_COMMITMENT,
                replaceRecentBlockhash: true
            });
            return simulation.value;
        }
        catch (error) {
            console.error('Transaction simulation failed:', error);
            return null;
        }
    }
    estimateProfit(simulation, principal, fee) {
        if (!simulation.accounts || simulation.accounts.length === 0) {
            return new anchor_1.BN(0);
        }
        const preBalance = simulation.accounts[0]?.lamports || 0;
        const postBalance = simulation.accounts[simulation.accounts.length - 1]?.lamports || 0;
        const balanceChange = new anchor_1.BN(postBalance - preBalance);
        return balanceChange.sub(fee);
    }
    calculateConfidence(simulation, provider) {
        let confidence = 0.5;
        if (!simulation.err) {
            confidence += 0.3;
        }
        if (simulation.unitsConsumed && simulation.unitsConsumed < this.MAX_COMPUTE_UNITS * 0.8) {
            confidence += 0.1;
        }
        if (provider.priority <= 2) {
            confidence += 0.1;
        }
        return Math.min(confidence, 1.0);
    }
    async executeWithRetry(transaction) {
        let lastError = null;
        for (let attempt = 0; attempt < this.MAX_RETRIES; attempt++) {
            try {
                const signature = await this.sendSmartTransaction(transaction);
                const confirmation = await this.confirmTransaction(signature);
                if (confirmation) {
                    return signature;
                }
            }
            catch (error) {
                lastError = error;
                console.warn(`Attempt ${attempt + 1} failed:`, error);
                if (attempt < this.MAX_RETRIES - 1) {
                    await this.exponentialBackoff(attempt);
                }
            }
        }
        throw lastError || new Error('Transaction failed after all retries');
    }
    async sendSmartTransaction(transaction) {
        const serialized = transaction.serialize();
        // Try Jito first for MEV protection
        try {
            const jitoEndpoint = 'https://mainnet.block-engine.jito.wtf/api/v1/transactions';
            const response = await fetch(jitoEndpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    jsonrpc: '2.0',
                    id: 1,
                    method: 'sendTransaction',
                    params: [
                        bs58_1.default.encode(serialized),
                        {
                            encoding: 'base58',
                            skipPreflight: true,
                            maxRetries: 0
                        }
                    ]
                })
            });
            const result = await response.json();
            if (result.result) {
                return result.result;
            }
        }
        catch (error) {
            console.warn('Jito submission failed, falling back to RPC:', error);
        }
        // Fallback to regular RPC
        return await this.connection.sendRawTransaction(serialized, {
            skipPreflight: true,
            maxRetries: 0
        });
    }
    async confirmTransaction(signature) {
        const startTime = Date.now();
        const timeout = 30000;
        while (Date.now() - startTime < timeout) {
            try {
                const status = await this.connection.getSignatureStatus(signature, {
                    searchTransactionHistory: true
                });
                if (status?.value?.confirmationStatus === 'confirmed' ||
                    status?.value?.confirmationStatus === 'finalized') {
                    return !status.value.err;
                }
                if (status?.value?.err) {
                    throw new Error(`Transaction failed: ${JSON.stringify(status.value.err)}`);
                }
            }
            catch (error) {
                console.warn('Error checking transaction status:', error);
            }
            await new Promise(resolve => setTimeout(resolve, 500));
        }
        throw new Error('Transaction confirmation timeout');
    }
    async getDynamicPriorityFee() {
        try {
            const recentPrioritizationFees = await this.connection.getRecentPrioritizationFees();
            if (recentPrioritizationFees.length === 0) {
                return 50000;
            }
            const fees = recentPrioritizationFees
                .map(fee => fee.prioritizationFee)
                .sort((a, b) => a - b);
            const percentileIndex = Math.floor(fees.length * (this.PRIORITY_FEE_PERCENTILE / 100));
            return Math.max(fees[percentileIndex], 10000);
        }
        catch (error) {
            console.warn('Failed to fetch dynamic priority fee:', error);
            return 50000;
        }
    }
    async fetchLookupTables(instructions) {
        const tableAddresses = new Set();
        const tables = [];
        // Extract lookup table addresses from instructions
        instructions.forEach(instruction => {
            instruction.keys.forEach(key => {
                const address = key.pubkey.toBase58();
                if (this.isLookupTableAddress(address)) {
                    tableAddresses.add(address);
                }
            });
        });
        // Fetch tables with caching
        for (const address of tableAddresses) {
            const cached = this.lookupTableCache.get(address);
            if (cached) {
                tables.push(cached);
            }
            else {
                try {
                    const tableAccount = await this.connection.getAddressLookupTable(new web3_js_1.PublicKey(address));
                    if (tableAccount.value) {
                        this.lookupTableCache.set(address, tableAccount.value);
                        tables.push(tableAccount.value);
                    }
                }
                catch (error) {
                    console.warn(`Failed to fetch lookup table ${address}:`, error);
                }
            }
        }
        return tables;
    }
    isLookupTableAddress(address) {
        const knownTables = [
            'F3MfgEJe1TApJiA14nN2m4uAH4EBVrqdBnHeGeSXvQ7B',
            '2immgwYhBEdQXtSTonXT7v7S1NMmeXCVhSLmgdgCHWzS',
            '9eZbWiHsPRsxLSiHxzg2pkXjAuQZLnCa6NcYvNKBfEfD'
        ];
        return knownTables.includes(address);
    }
    getRandomJitoTipAccount() {
        const index = Math.floor(Math.random() * this.JITO_TIP_ACCOUNTS.length);
        return this.JITO_TIP_ACCOUNTS[index];
    }
    async exponentialBackoff(attempt) {
        const baseDelay = 100;
        const maxDelay = 5000;
        const delay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
        await new Promise(resolve => setTimeout(resolve, delay));
    }
    async getProviderStats() {
        const stats = new Map();
        for (const [name, provider] of this.providers) {
            try {
                const account = await this.connection.getAccountInfo(provider.loanAccount);
                const balance = account ? account.lamports : 0;
                stats.set(name, {
                    name: provider.name,
                    available: balance > 0,
                    maxLoan: provider.maxLoanAmount.toString(),
                    feeRate: provider.feeRate,
                    priority: provider.priority
                });
            }
            catch (error) {
                stats.set(name, {
                    name: provider.name,
                    available: false,
                    error: error.message
                });
            }
        }
        return stats;
    }
    async healthCheck() {
        try {
            const slot = await this.connection.getSlot();
            const balance = await this.connection.getBalance(this.wallet.publicKey);
            if (balance < 10000000) {
                console.warn(`Low wallet balance: ${balance / 1000000000} SOL`);
                return false;
            }
            const providerStats = await this.getProviderStats();
            const availableProviders = Array.from(providerStats.values()).filter(stat => stat.available);
            if (availableProviders.length === 0) {
                console.error('No flash loan providers available');
                return false;
            }
            console.log(`Health check passed. Slot: ${slot}, Providers: ${availableProviders.length}`);
            return true;
        }
        catch (error) {
            console.error('Health check failed:', error);
            return false;
        }
    }
    updateProvider(name, updates) {
        const provider = this.providers.get(name);
        if (provider) {
            this.providers.set(name, { ...provider, ...updates });
        }
    }
    clearLookupTableCache() {
        this.lookupTableCache.clear();
    }
    async preflightChecks(params) {
        try {
            // Check wallet balance
            const balance = await this.connection.getBalance(this.wallet.publicKey);
            if (balance < 50000000) { // 0.05 SOL minimum
                throw new Error(`Insufficient balance: ${balance / 1000000000} SOL`);
            }
            // Check token account exists
            const tokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(params.tokenMint, this.wallet.publicKey);
            try {
                const account = await (0, spl_token_1.getAccount)(this.connection, tokenAccount);
                if (account.amount < params.amount.toNumber()) {
                    console.warn('Token account balance lower than loan amount');
                }
            }
            catch (error) {
                console.log('Token account will be created during transaction');
            }
            // Validate instructions
            if (params.targetInstructions.length === 0) {
                throw new Error('No target instructions provided');
            }
            // Check network conditions
            const recentPerf = await this.connection.getRecentPerformanceSamples(1);
            if (recentPerf.length > 0 && recentPerf[0].numTransactions < 1000) {
                console.warn('Low network activity detected');
            }
            return true;
        }
        catch (error) {
            console.error('Preflight checks failed:', error);
            return false;
        }
    }
    getOptimalTiming() {
        const now = new Date();
        const hour = now.getUTCHours();
        const minute = now.getUTCMinutes();
        // Peak times: 14:00-22:00 UTC
        const isPeakTime = hour >= 14 && hour < 22;
        // Avoid on-the-hour executions due to scheduled bots
        const isNearHour = minute < 2 || minute > 58;
        if (isNearHour) {
            return { execute: false, reason: 'Too close to hour mark' };
        }
        if (!isPeakTime) {
            return { execute: true, reason: 'Off-peak hours - lower competition' };
        }
        return { execute: true, reason: 'Normal execution window' };
    }
    async getNetworkCongestion() {
        try {
            const samples = await this.connection.getRecentPerformanceSamples(5);
            if (samples.length === 0)
                return 0.5;
            const avgTps = samples.reduce((sum, sample) => sum + (sample.numTransactions / sample.samplePeriodSecs), 0) / samples.length;
            // Normalize to 0-1 scale (assuming 3000 TPS is high congestion)
            return Math.min(avgTps / 3000, 1);
        }
        catch (error) {
            console.warn('Failed to get network congestion:', error);
            return 0.5;
        }
    }
    formatAmount(amount, decimals) {
        const divisor = new anchor_1.BN(10).pow(new anchor_1.BN(decimals));
        const quotient = amount.div(divisor);
        const remainder = amount.mod(divisor);
        const remainderStr = remainder.toString().padStart(decimals, '0');
        return `${quotient.toString()}.${remainderStr}`;
    }
    async emergencyWithdraw(tokenMint) {
        try {
            const tokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(tokenMint, this.wallet.publicKey);
            const account = await (0, spl_token_1.getAccount)(this.connection, tokenAccount);
            if (account.amount === 0n) {
                throw new Error('No tokens to withdraw');
            }
            // TODO: Add withdrawal logic here
            throw new Error("Emergency withdraw not implemented");
        }
        catch (error) {
            console.error("Emergency withdraw failed:", error);
            throw error;
        }
    }
}
exports.FlashLoanRouter = FlashLoanRouter;
exports.DEFAULT_SLIPPAGE_BPS = 50; // 0.5%
exports.DEFAULT_PRIORITY_FEE = 50000; // microLamports
exports.MIN_PROFIT_THRESHOLD = new anchor_1.BN(1000000); // 0.001 SOL
// Error types for better error handling
class InsufficientLiquidityError extends Error {
    constructor(message) {
        super(message);
        this.name = "InsufficientLiquidityError";
    }
}
exports.InsufficientLiquidityError = InsufficientLiquidityError;
class FlashLoanError extends Error {
    constructor(message) {
        super(message);
        this.name = "FlashLoanError";
        this.totalProfit = new anchor_1.BN(0);
        this.averageExecutionTime = 0;
        this.providerStats = new Map();
    }
}
exports.FlashLoanError = FlashLoanError;
// Validation utilities
exports.ValidationUtils = {
    isValidAmount(amount) {
        return amount.gt(new anchor_1.BN(0)) && amount.lt(new anchor_1.BN(Number.MAX_SAFE_INTEGER));
    },
    isValidTokenMint(mint) {
        const validMints = [
            'So11111111111111111111111111111111111111112', // SOL
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
            'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So', // mSOL
            '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs' // ETH
        ];
        return validMints.includes(mint.toBase58());
    },
    validateParams(params) {
        if (!this.isValidAmount(params.amount)) {
            throw new FlashLoanError('Invalid loan amount');
        }
        if (!this.isValidTokenMint(params.tokenMint)) {
            throw new FlashLoanError('Unsupported token mint');
        }
        if (params.targetInstructions.length === 0) {
            throw new FlashLoanError('No target instructions provided');
        }
        if (params.targetInstructions.length > 20) {
            throw new FlashLoanError('Too many instructions');
        }
        if (params.slippageBps && (params.slippageBps < 0 || params.slippageBps > 1000)) {
            throw new FlashLoanError('Invalid slippage (0-1000 bps)');
        }
    }
};
// Production-ready monitoring class
class FlashLoanMonitor {
    constructor() {
        this.metrics = {
            totalAttempts: 0,
            successfulLoans: 0,
            failedLoans: 0,
            totalProfit: new anchor_1.BN(0),
            averageExecutionTime: 0,
            providerStats: new Map()
        };
    }
    recordAttempt(provider) {
        this.metrics.totalAttempts++;
        const stats = this.metrics.providerStats.get(provider) || {
            attempts: 0,
            successes: 0,
            failures: 0,
            totalFees: new anchor_1.BN(0)
        };
        stats.attempts++;
        this.metrics.providerStats.set(provider, stats);
    }
    recordSuccess(provider, profit, fee, executionTime) {
        this.metrics.successfulLoans++;
        this.metrics.totalProfit = this.metrics.totalProfit.add(profit);
        const stats = this.metrics.providerStats.get(provider);
        stats.successes++;
        stats.totalFees = stats.totalFees.add(fee);
        this.metrics.providerStats.set(provider, stats);
        // Update average execution time
        const totalTime = this.metrics.averageExecutionTime * (this.metrics.successfulLoans - 1) + executionTime;
        this.metrics.averageExecutionTime = totalTime / this.metrics.successfulLoans;
    }
    recordFailure(provider) {
        this.metrics.failedLoans++;
        const stats = this.metrics.providerStats.get(provider);
        stats.failures++;
        this.metrics.providerStats.set(provider, stats);
    }
    getMetrics() {
        return { ...this.metrics };
    }
    reset() {
        this.metrics = {
            totalAttempts: 0,
            successfulLoans: 0,
            failedLoans: 0,
            totalProfit: new anchor_1.BN(0),
            averageExecutionTime: 0,
            providerStats: new Map()
        };
    }
}
exports.FlashLoanMonitor = FlashLoanMonitor;
// Utility function for calculating optimal loan amounts
function calculateOptimalLoanAmount(availableLiquidity, maxLoanAmount, targetProfit, feeRate) {
    const maxPossible = anchor_1.BN.min(availableLiquidity, maxLoanAmount);
    const feeAdjustedTarget = targetProfit.mul(new anchor_1.BN(10000)).div(new anchor_1.BN(10000 - feeRate * 10000));
    return anchor_1.BN.min(maxPossible, feeAdjustedTarget);
}
// Production safety checks
async function performSafetyChecks(connection, wallet) {
    const warnings = [];
    try {
        // Check wallet balance
        const balance = await connection.getBalance(wallet);
        if (balance < 100000000) { // 0.1 SOL
            warnings.push('Low SOL balance for operations');
        }
        // Check RPC health
        const slot = await connection.getSlot();
        const blockTime = await connection.getBlockTime(slot);
        if (!blockTime || Date.now() / 1000 - blockTime > 60) {
            warnings.push('RPC might be lagging');
        }
        // Check recent blockhash validity
        const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash();
        const currentBlockHeight = await connection.getBlockHeight();
        if (lastValidBlockHeight - currentBlockHeight < 150) {
            warnings.push('Close to blockhash expiration');
        }
        return {
            safe: warnings.length === 0,
            warnings
        };
    }
    catch (error) {
        return {
            safe: false,
            warnings: [`Safety check failed: ${error.message}`]
        };
    }
}
// Export a ready-to-use singleton instance factory
class FlashLoanRouterFactory {
    static create(connection, wallet, config) {
        if (!this.instance) {
            this.instance = new FlashLoanRouter(connection, wallet);
            // Apply custom configuration if provided
            if (config?.customProviders) {
                config.customProviders.forEach(provider => {
                    this.instance.updateProvider(provider.name, provider);
                });
            }
        }
        return this.instance;
    }
    static destroy() {
        this.instance = null;
    }
}
exports.FlashLoanRouterFactory = FlashLoanRouterFactory;
FlashLoanRouterFactory.instance = null;
// Version information for tracking
exports.VERSION = '1.0.0';
exports.BUILD_DATE = new Date().toISOString();
// Performance optimization settings
exports.PERFORMANCE_CONFIG = {
    PARALLEL_SIMULATIONS: 3,
    CACHE_TTL_MS: 60000, // 1 minute
    MIN_PROFIT_USD: 1, // $1 minimum profit
    GAS_BUFFER_MULTIPLIER: 1.1,
    BLOCKHASH_REFRESH_INTERVAL: 30000 // 30 seconds
};
// Initialize module-level safety checks
let isInitialized = false;
async function initialize(connection) {
    if (isInitialized)
        return;
    try {
        // Verify connection
        const version = await connection.getVersion();
        console.log(`Connected to Solana ${version['solana-core']}`);
        // Check required programs exist
        const requiredPrograms = [
            spl_token_1.TOKEN_PROGRAM_ID,
            spl_token_1.ASSOCIATED_TOKEN_PROGRAM_ID,
            new web3_js_1.PublicKey('So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo'), // Solend
            new web3_js_1.PublicKey('Port7uDYB3wk5GJAw4c4QHXoPgaCQ3LST4gfgnM4n1w'), // Port
            new web3_js_1.PublicKey('KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD') // Kamino
        ];
        for (const programId of requiredPrograms) {
            const info = await connection.getAccountInfo(programId);
            if (!info) {
                throw new Error(`Required program ${programId.toBase58()} not found`);
            }
        }
        isInitialized = true;
        console.log('FlashLoanRouter initialized successfully');
    }
    catch (error) {
        console.error('Initialization failed:', error);
        throw error;
    }
}
// Cleanup function for graceful shutdown
async function cleanup() {
    FlashLoanRouterFactory.destroy();
    isInitialized = false;
    console.log('FlashLoanRouter cleanup completed');
}
// Default export for convenience
exports.default = FlashLoanRouter;
// Type guard functions
function isFlashLoanError(error) {
    return error instanceof FlashLoanError;
}
function isInsufficientLiquidityError(error) {
    return error instanceof InsufficientLiquidityError;
}
// Final runtime validation
// Module ready indicator
exports.MODULE_READY = true;
//# sourceMappingURL=FlashLoanRouter.js.map