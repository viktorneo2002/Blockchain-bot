"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.FlashLoanOrchestrator = void 0;
const web3_js_1 = require("@solana/web3.js");
const spl_token_1 = require("@solana/spl-token");
const anchor_1 = require("@project-serum/anchor");
const events_1 = require("events");
class FlashLoanOrchestrator extends events_1.EventEmitter {
    constructor(connection, wallet, providerConfig) {
        super();
        this.maxRetries = 3;
        this.defaultTimeout = 30000;
        this.maxConcurrentLoans = 5;
        this.connection = connection;
        this.wallet = wallet;
        this.providers = new Map();
        this.activeLoans = new Set();
        this.performanceMetrics = new Map();
        this.circuitBreaker = new Map();
        this.initializeProviders(providerConfig);
    }
    initializeProviders(config) {
        // Solend Flash Loan Provider
        if (config?.solend) {
            this.providers.set('solend', {
                name: 'Solend',
                programId: new web3_js_1.PublicKey(config.solend.programId),
                maxLoanAmount: new anchor_1.BN(1000000 * web3_js_1.LAMPORTS_PER_SOL),
                feeRate: 0.0009,
                priority: 1,
                isActive: true
            });
        }
        // Port Finance Flash Loan Provider
        if (config?.portFinance) {
            this.providers.set('port', {
                name: 'Port Finance',
                programId: new web3_js_1.PublicKey(config.portFinance.programId),
                maxLoanAmount: new anchor_1.BN(500000 * web3_js_1.LAMPORTS_PER_SOL),
                feeRate: 0.001,
                priority: 2,
                isActive: true
            });
        }
        // Mango Markets Flash Loan Provider
        if (config?.mango) {
            this.providers.set('mango', {
                name: 'Mango Markets',
                programId: new web3_js_1.PublicKey(config.mango.programId),
                maxLoanAmount: new anchor_1.BN(750000 * web3_js_1.LAMPORTS_PER_SOL),
                feeRate: 0.0008,
                priority: 3,
                isActive: true
            });
        }
        // Custom providers
        config?.custom?.forEach((provider, index) => {
            this.providers.set(`custom_${index}`, {
                name: provider.name,
                programId: new web3_js_1.PublicKey(provider.programId),
                maxLoanAmount: new anchor_1.BN(100000 * web3_js_1.LAMPORTS_PER_SOL),
                feeRate: 0.001,
                priority: provider.priority || 99,
                isActive: true
            });
        });
    }
    async executeFlashLoan(request) {
        const startTime = Date.now();
        const loanId = this.generateLoanId();
        if (this.activeLoans.size >= this.maxConcurrentLoans) {
            return {
                success: false,
                error: new Error('Maximum concurrent loans reached'),
                timestamp: Date.now()
            };
        }
        this.activeLoans.add(loanId);
        this.emit('loan:started', { loanId, request });
        try {
            // Validate request
            this.validateRequest(request);
            // Select optimal provider
            const provider = await this.selectOptimalProvider(request);
            if (!provider) {
                throw new Error('No suitable flash loan provider available');
            }
            // Build flash loan transaction
            const transaction = await this.buildFlashLoanTransaction(provider, request);
            // Execute with retry logic
            const result = await this.executeWithRetry(transaction, request, provider);
            // Update metrics
            this.updateMetrics(provider.name, Date.now() - startTime);
            this.emit('loan:completed', { loanId, result });
            return result;
        }
        catch (error) {
            this.handleProviderFailure(loanId, error);
            return {
                success: false,
                error: error,
                timestamp: Date.now()
            };
        }
        finally {
            this.activeLoans.delete(loanId);
        }
    }
    validateRequest(request) {
        if (request.amount.lte(new anchor_1.BN(0))) {
            throw new Error('Loan amount must be greater than 0');
        }
        if (request.targetInstructions.length === 0) {
            throw new Error('No target instructions provided');
        }
        if (request.maxSlippage < 0 || request.maxSlippage > 1) {
            throw new Error('Max slippage must be between 0 and 1');
        }
        if (request.priorityFee < 0) {
            throw new Error('Priority fee cannot be negative');
        }
    }
    async selectOptimalProvider(request) {
        const eligibleProviders = Array.from(this.providers.values())
            .filter(provider => {
            // Check if provider is active
            if (!provider.isActive)
                return false;
            // Check circuit breaker
            const breaker = this.circuitBreaker.get(provider.name);
            if (breaker && breaker.failures >= 3) {
                const timeSinceFailure = Date.now() - breaker.lastFailure;
                if (timeSinceFailure < 300000)
                    return false; // 5 minute cooldown
            }
            // Check loan amount limits
            if (request.amount.gt(provider.maxLoanAmount))
                return false;
            return true;
        })
            .sort((a, b) => {
            // Sort by: fee rate, performance, then priority
            const feeComparison = a.feeRate - b.feeRate;
            if (Math.abs(feeComparison) > 0.0001)
                return feeComparison;
            const aPerf = this.getAveragePerformance(a.name);
            const bPerf = this.getAveragePerformance(b.name);
            const perfComparison = aPerf - bPerf;
            if (Math.abs(perfComparison) > 10)
                return perfComparison;
            return a.priority - b.priority;
        });
        return eligibleProviders[0] || null;
    }
    async buildFlashLoanTransaction(provider, request) {
        const instructions = [];
        // Add compute budget instructions
        instructions.push(web3_js_1.ComputeBudgetProgram.setComputeUnitLimit({
            units: 1400000
        }), web3_js_1.ComputeBudgetProgram.setComputeUnitPrice({
            microLamports: request.priorityFee
        }));
        // Get or create token accounts
        const userTokenAccount = await this.getOrCreateTokenAccount(request.tokenMint);
        // Build provider-specific flash loan instructions
        const flashLoanInstructions = await this.buildProviderInstructions(provider, request, userTokenAccount);
        instructions.push(...flashLoanInstructions.borrowInstructions);
        instructions.push(...request.targetInstructions);
        instructions.push(...flashLoanInstructions.repayInstructions);
        // Build versioned transaction
        const { blockhash } = await this.connection.getLatestBlockhash('finalized');
        const message = new web3_js_1.TransactionMessage({
            payerKey: this.wallet.publicKey,
            recentBlockhash: blockhash,
            instructions
        }).compileToV0Message();
        const transaction = new web3_js_1.VersionedTransaction(message);
        transaction.sign([this.wallet]);
        return transaction;
    }
    async buildProviderInstructions(provider, request, userTokenAccount) {
        switch (provider.name.toLowerCase()) {
            case 'solend':
                return this.buildSolendInstructions(provider, request, userTokenAccount);
            case 'port finance':
                return this.buildPortInstructions(provider, request, userTokenAccount);
            case 'mango markets':
                return this.buildMangoInstructions(provider, request, userTokenAccount);
            default:
                return this.buildGenericInstructions(provider, request, userTokenAccount);
        }
    }
    buildSolendInstructions(provider, request, userTokenAccount) {
        // Solend-specific flash loan instruction building
        const borrowIx = new web3_js_1.TransactionInstruction({
            programId: provider.programId,
            keys: [
                { pubkey: userTokenAccount, isSigner: false, isWritable: true },
                { pubkey: request.tokenMint, isSigner: false, isWritable: false },
                { pubkey: this.wallet.publicKey, isSigner: true, isWritable: false }
            ],
            data: Buffer.concat([
                Buffer.from([0x01]), // Flash loan instruction
                request.amount.toArrayLike(Buffer, 'le', 8)
            ])
        });
        const feeAmount = request.amount.muln(provider.feeRate * 10000).divn(10000);
        const repayAmount = request.amount.add(feeAmount);
        const repayIx = new web3_js_1.TransactionInstruction({
            programId: provider.programId,
            keys: [
                { pubkey: userTokenAccount, isSigner: false, isWritable: true },
                { pubkey: request.tokenMint, isSigner: false, isWritable: false },
                { pubkey: this.wallet.publicKey, isSigner: true, isWritable: false }
            ],
            data: Buffer.concat([
                Buffer.from([0x02]), // Repay instruction
                repayAmount.toArrayLike(Buffer, 'le', 8)
            ])
        });
        return {
            borrowInstructions: [borrowIx],
            repayInstructions: [repayIx]
        };
    }
    buildPortInstructions(provider, request, userTokenAccount) {
        // Port Finance-specific implementation
        const borrowData = Buffer.concat([
            Buffer.from([0x03]), // Port flash loan opcode
            request.amount.toArrayLike(Buffer, 'le', 8),
            Buffer.from([request.maxSlippage * 100]) // Slippage as percentage
        ]);
        const borrowIx = new web3_js_1.TransactionInstruction({
            programId: provider.programId,
            keys: [
                { pubkey: userTokenAccount, isSigner: false, isWritable: true },
                { pubkey: request.tokenMint, isSigner: false, isWritable: false },
                { pubkey: this.wallet.publicKey, isSigner: true, isWritable: true },
                { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false }
            ],
            data: borrowData
        });
        const feeAmount = request.amount.muln(provider.feeRate * 10000).divn(10000);
        const repayAmount = request.amount.add(feeAmount);
        const repayIx = new web3_js_1.TransactionInstruction({
            programId: provider.programId,
            keys: [
                { pubkey: userTokenAccount, isSigner: false, isWritable: true },
                { pubkey: request.tokenMint, isSigner: false, isWritable: false },
                { pubkey: this.wallet.publicKey, isSigner: true, isWritable: true }
            ],
            data: Buffer.concat([
                Buffer.from([0x04]), // Port repay opcode
                repayAmount.toArrayLike(Buffer, 'le', 8)
            ])
        });
        return {
            borrowInstructions: [borrowIx],
            repayInstructions: [repayIx]
        };
    }
    buildMangoInstructions(provider, request, userTokenAccount) {
        // Mango Markets-specific implementation
        const borrowData = Buffer.concat([
            Buffer.from([0x05]), // Mango flash loan instruction
            request.amount.toArrayLike(Buffer, 'le', 8),
            Buffer.from([0x00]) // Flash loan type
        ]);
        const borrowIx = new web3_js_1.TransactionInstruction({
            programId: provider.programId,
            keys: [
                { pubkey: userTokenAccount, isSigner: false, isWritable: true },
                { pubkey: request.tokenMint, isSigner: false, isWritable: false },
                { pubkey: this.wallet.publicKey, isSigner: true, isWritable: true },
                { pubkey: web3_js_1.SystemProgram.programId, isSigner: false, isWritable: false },
                { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false }
            ],
            data: borrowData
        });
        const feeAmount = request.amount.muln(provider.feeRate * 10000).divn(10000);
        const repayAmount = request.amount.add(feeAmount);
        const repayData = Buffer.concat([
            Buffer.from([0x06]), // Mango repay instruction
            repayAmount.toArrayLike(Buffer, 'le', 8)
        ]);
        const repayIx = new web3_js_1.TransactionInstruction({
            programId: provider.programId,
            keys: [
                { pubkey: userTokenAccount, isSigner: false, isWritable: true },
                { pubkey: request.tokenMint, isSigner: false, isWritable: false },
                { pubkey: this.wallet.publicKey, isSigner: true, isWritable: true }
            ],
            data: repayData
        });
        return {
            borrowInstructions: [borrowIx],
            repayInstructions: [repayIx]
        };
    }
    buildGenericInstructions(provider, request, userTokenAccount) {
        // Generic flash loan provider implementation
        const borrowIx = new web3_js_1.TransactionInstruction({
            programId: provider.programId,
            keys: [
                { pubkey: userTokenAccount, isSigner: false, isWritable: true },
                { pubkey: request.tokenMint, isSigner: false, isWritable: false },
                { pubkey: this.wallet.publicKey, isSigner: true, isWritable: true }
            ],
            data: Buffer.concat([
                Buffer.from([0x00]), // Generic borrow
                request.amount.toArrayLike(Buffer, 'le', 8)
            ])
        });
        const feeAmount = request.amount.muln(provider.feeRate * 10000).divn(10000);
        const repayAmount = request.amount.add(feeAmount);
        const repayIx = new web3_js_1.TransactionInstruction({
            programId: provider.programId,
            keys: [
                { pubkey: userTokenAccount, isSigner: false, isWritable: true },
                { pubkey: request.tokenMint, isSigner: false, isWritable: false },
                { pubkey: this.wallet.publicKey, isSigner: true, isWritable: true }
            ],
            data: Buffer.concat([
                Buffer.from([0x01]), // Generic repay
                repayAmount.toArrayLike(Buffer, 'le', 8)
            ])
        });
        return {
            borrowInstructions: [borrowIx],
            repayInstructions: [repayIx]
        };
    }
    async getOrCreateTokenAccount(mint) {
        const associatedTokenAddress = await (0, spl_token_1.getAssociatedTokenAddress)(mint, this.wallet.publicKey, false, spl_token_1.TOKEN_PROGRAM_ID, spl_token_1.ASSOCIATED_TOKEN_PROGRAM_ID);
        try {
            await (0, spl_token_1.getAccount)(this.connection, associatedTokenAddress, 'confirmed', spl_token_1.TOKEN_PROGRAM_ID);
            return associatedTokenAddress;
        }
        catch (error) {
            if (error instanceof spl_token_1.TokenAccountNotFoundError) {
                const createAtaIx = (0, spl_token_1.createAssociatedTokenAccountInstruction)(this.wallet.publicKey, associatedTokenAddress, this.wallet.publicKey, mint, spl_token_1.TOKEN_PROGRAM_ID, spl_token_1.ASSOCIATED_TOKEN_PROGRAM_ID);
                const { blockhash } = await this.connection.getLatestBlockhash();
                const transaction = new web3_js_1.Transaction().add(createAtaIx);
                transaction.recentBlockhash = blockhash;
                transaction.feePayer = this.wallet.publicKey;
                transaction.sign(this.wallet);
                await this.connection.sendRawTransaction(transaction.serialize(), {
                    skipPreflight: false,
                    preflightCommitment: 'confirmed'
                });
                await this.connection.confirmTransaction({
                    signature: transaction.signature.toString(),
                    blockhash,
                    lastValidBlockHeight: (await this.connection.getBlockHeight()) + 150
                });
            }
            return associatedTokenAddress;
        }
    }
    async executeWithRetry(transaction, request, provider) {
        let lastError;
        const timeout = request.timeout || this.defaultTimeout;
        for (let attempt = 0; attempt < this.maxRetries; attempt++) {
            try {
                const sendOptions = {
                    skipPreflight: false,
                    preflightCommitment: 'processed',
                    maxRetries: 0
                };
                const signature = await Promise.race([
                    this.connection.sendTransaction(transaction, sendOptions),
                    new Promise((_, reject) => setTimeout(() => reject(new Error('Transaction timeout')), timeout))
                ]);
                const { blockhash, lastValidBlockHeight } = await this.connection.getLatestBlockhash('finalized');
                const confirmation = await this.connection.confirmTransaction({
                    signature,
                    blockhash,
                    lastValidBlockHeight
                }, 'confirmed');
                if (confirmation.value.err) {
                    throw new Error(`Transaction failed: ${confirmation.value.err}`);
                }
                // Parse transaction result for profit calculation
                const txDetails = await this.connection.getTransaction(signature, {
                    commitment: 'confirmed',
                    maxSupportedTransactionVersion: 0
                });
                const profit = await this.calculateProfit(txDetails, request, provider);
                const gasUsed = new anchor_1.BN(txDetails?.meta?.fee || 0);
                return {
                    success: true,
                    txSignature: signature,
                    profit,
                    gasUsed,
                    provider: provider.name,
                    timestamp: Date.now()
                };
            }
            catch (error) {
                lastError = error;
                this.emit('loan:retry', {
                    attempt: attempt + 1,
                    error: lastError.message,
                    provider: provider.name
                });
                if (attempt < this.maxRetries - 1) {
                    await this.delay(Math.pow(2, attempt) * 1000);
                }
            }
        }
        throw lastError || new Error('Flash loan execution failed');
    }
    async calculateProfit(txDetails, request, provider) {
        if (!txDetails || !txDetails.meta) {
            return new anchor_1.BN(0);
        }
        try {
            const preBalances = txDetails.meta.preTokenBalances || [];
            const postBalances = txDetails.meta.postTokenBalances || [];
            const walletIndex = txDetails.transaction.message.accountKeys.findIndex((key) => key.equals(this.wallet.publicKey));
            let totalProfit = new anchor_1.BN(0);
            for (let i = 0; i < postBalances.length; i++) {
                if (postBalances[i].owner === this.wallet.publicKey.toBase58()) {
                    const preBalance = preBalances.find((b) => b.accountIndex === postBalances[i].accountIndex);
                    if (preBalance) {
                        const diff = new anchor_1.BN(postBalances[i].uiTokenAmount.amount)
                            .sub(new anchor_1.BN(preBalance.uiTokenAmount.amount));
                        if (diff.gt(new anchor_1.BN(0))) {
                            totalProfit = totalProfit.add(diff);
                        }
                    }
                }
            }
            // Subtract flash loan fee
            const feeAmount = request.amount.muln(provider.feeRate * 10000).divn(10000);
            totalProfit = totalProfit.sub(feeAmount);
            // Subtract gas fees
            const gasFee = new anchor_1.BN(txDetails.meta.fee);
            totalProfit = totalProfit.sub(gasFee);
            return totalProfit;
        }
        catch (error) {
            this.emit('profit:calculation:error', error);
            return new anchor_1.BN(0);
        }
    }
    updateMetrics(providerName, executionTime) {
        if (!this.performanceMetrics.has(providerName)) {
            this.performanceMetrics.set(providerName, []);
        }
        const metrics = this.performanceMetrics.get(providerName);
        metrics.push(executionTime);
        // Keep only last 100 metrics
        if (metrics.length > 100) {
            metrics.shift();
        }
        this.emit('metrics:updated', {
            provider: providerName,
            avgTime: this.getAveragePerformance(providerName),
            lastTime: executionTime
        });
    }
    getAveragePerformance(providerName) {
        const metrics = this.performanceMetrics.get(providerName);
        if (!metrics || metrics.length === 0)
            return 0;
        const sum = metrics.reduce((a, b) => a + b, 0);
        return sum / metrics.length;
    }
    handleProviderFailure(loanId, error) {
        const providerName = this.getProviderFromError(error);
        if (providerName) {
            if (!this.circuitBreaker.has(providerName)) {
                this.circuitBreaker.set(providerName, { failures: 0, lastFailure: 0 });
            }
            const breaker = this.circuitBreaker.get(providerName);
            breaker.failures++;
            breaker.lastFailure = Date.now();
            if (breaker.failures >= 3) {
                this.emit('provider:circuitBreaker:triggered', {
                    provider: providerName,
                    failures: breaker.failures
                });
            }
        }
        this.emit('loan:failed', { loanId, error: error.message });
    }
    getProviderFromError(error) {
        // Extract provider name from error message or stack trace
        for (const [name, provider] of this.providers) {
            if (error.message.includes(provider.name) ||
                error.stack?.includes(provider.programId.toBase58())) {
                return name;
            }
        }
        return null;
    }
    generateLoanId() {
        return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    }
    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    async healthCheck() {
        const providerStatuses = Array.from(this.providers.entries()).map(([key, provider]) => {
            const breaker = this.circuitBreaker.get(key);
            const isTriggered = breaker && breaker.failures >= 3 &&
                (Date.now() - breaker.lastFailure) < 300000;
            return {
                name: provider.name,
                active: provider.isActive && !isTriggered,
                avgPerformance: this.getAveragePerformance(key),
                circuitBreakerStatus: isTriggered ? 'triggered' : 'normal'
            };
        });
        const healthy = providerStatuses.some(p => p.active) &&
            this.activeLoans.size < this.maxConcurrentLoans;
        return {
            healthy,
            providers: providerStatuses,
            activeLoans: this.activeLoans.size
        };
    }
    updateProviderStatus(providerName, isActive) {
        const provider = Array.from(this.providers.values())
            .find(p => p.name === providerName);
        if (provider) {
            provider.isActive = isActive;
            this.emit('provider:status:updated', { provider: providerName, isActive });
        }
    }
    resetCircuitBreaker(providerName) {
        if (providerName) {
            this.circuitBreaker.delete(providerName);
        }
        else {
            this.circuitBreaker.clear();
        }
        this.emit('circuitBreaker:reset', { provider: providerName || 'all' });
    }
    getMetrics() {
        let totalLoans = 0;
        let totalTime = 0;
        let topProvider = null;
        let bestAvgTime = Infinity;
        for (const [provider, metrics] of this.performanceMetrics) {
            totalLoans += metrics.length;
            totalTime += metrics.reduce((a, b) => a + b, 0);
            const avgTime = this.getAveragePerformance(provider);
            if (avgTime < bestAvgTime && avgTime > 0) {
                bestAvgTime = avgTime;
                topProvider = provider;
            }
        }
        return {
            totalLoansExecuted: totalLoans,
            averageExecutionTime: totalLoans > 0 ? totalTime / totalLoans : 0,
            successRate: totalLoans > 0 ?
                (totalLoans - Array.from(this.circuitBreaker.values())
                    .reduce((sum, b) => sum + b.failures, 0)) / totalLoans : 0,
            topProvider
        };
    }
    async simulateFlashLoan(request) {
        const warnings = [];
        try {
            // Validate request first
            this.validateRequest(request);
            // Select provider
            const provider = await this.selectOptimalProvider(request);
            if (!provider) {
                return {
                    feasible: false,
                    estimatedProfit: new anchor_1.BN(0),
                    estimatedGas: new anchor_1.BN(0),
                    bestProvider: null,
                    warnings: ['No suitable provider available']
                };
            }
            // Build transaction for simulation
            const transaction = await this.buildFlashLoanTransaction(provider, request);
            // Simulate transaction
            const simulation = await this.connection.simulateTransaction(transaction, {
                sigVerify: false,
                commitment: 'processed'
            });
            if (simulation.value.err) {
                warnings.push(`Simulation error: ${JSON.stringify(simulation.value.err)}`);
                return {
                    feasible: false,
                    estimatedProfit: new anchor_1.BN(0),
                    estimatedGas: new anchor_1.BN(simulation.value.unitsConsumed || 0),
                    bestProvider: provider,
                    warnings
                };
            }
            // Calculate estimated profit
            const feeAmount = request.amount.muln(provider.feeRate * 10000).divn(10000);
            const estimatedGas = new anchor_1.BN(simulation.value.unitsConsumed || 200000)
                .muln(request.priorityFee)
                .divn(1000000);
            // This is a rough estimate - actual profit depends on arbitrage execution
            const estimatedProfit = new anchor_1.BN(0).sub(feeAmount).sub(estimatedGas);
            if (simulation.value.unitsConsumed && simulation.value.unitsConsumed > 1200000) {
                warnings.push('High compute unit consumption detected');
            }
            return {
                feasible: true,
                estimatedProfit,
                estimatedGas,
                bestProvider: provider,
                warnings
            };
        }
        catch (error) {
            warnings.push(`Simulation failed: ${error.message}`);
            return {
                feasible: false,
                estimatedProfit: new anchor_1.BN(0),
                estimatedGas: new anchor_1.BN(0),
                bestProvider: null,
                warnings
            };
        }
    }
    async emergencyShutdown() {
        this.emit('emergency:shutdown:initiated');
        // Wait for active loans to complete (with timeout)
        const shutdownTimeout = 10000; // 10 seconds
        const startTime = Date.now();
        while (this.activeLoans.size > 0 && (Date.now() - startTime) < shutdownTimeout) {
            await this.delay(100);
        }
        if (this.activeLoans.size > 0) {
            this.emit('emergency:shutdown:forced', {
                remainingLoans: Array.from(this.activeLoans)
            });
        }
        // Deactivate all providers
        for (const provider of this.providers.values()) {
            provider.isActive = false;
        }
        // Clear all metrics and state
        this.performanceMetrics.clear();
        this.circuitBreaker.clear();
        this.activeLoans.clear();
        this.emit('emergency:shutdown:completed');
    }
    addCustomProvider(name, programId, config) {
        const key = `custom_${name.toLowerCase().replace(/\s+/g, '_')}`;
        this.providers.set(key, {
            name,
            programId: new web3_js_1.PublicKey(programId),
            maxLoanAmount: config.maxLoanAmount,
            feeRate: config.feeRate,
            priority: config.priority || 99,
            isActive: true
        });
        this.emit('provider:added', { name, programId });
    }
    removeProvider(providerName) {
        const key = Array.from(this.providers.entries())
            .find(([_, p]) => p.name === providerName)?.[0];
        if (key) {
            this.providers.delete(key);
            this.performanceMetrics.delete(key);
            this.circuitBreaker.delete(key);
            this.emit('provider:removed', { name: providerName });
            return true;
        }
        return false;
    }
    getProviderStats(providerName) {
        const entry = Array.from(this.providers.entries())
            .find(([_, p]) => p.name === providerName);
        if (!entry)
            return null;
        const [key, provider] = entry;
        const metrics = this.performanceMetrics.get(key) || [];
        const breaker = this.circuitBreaker.get(key);
        return {
            name: provider.name,
            programId: provider.programId.toBase58(),
            maxLoanAmount: provider.maxLoanAmount.toString(),
            feeRate: provider.feeRate,
            priority: provider.priority,
            isActive: provider.isActive,
            performance: {
                avgExecutionTime: this.getAveragePerformance(key),
                totalExecutions: metrics.length,
                failures: breaker?.failures || 0,
                lastFailure: breaker?.lastFailure || null
            }
        };
    }
    async optimizeGasSettings(recentPriorityFees) {
        if (recentPriorityFees.length === 0) {
            return { recommendedFee: 1000, confidence: 0.1 };
        }
        // Calculate percentiles
        const sorted = recentPriorityFees.sort((a, b) => a - b);
        const p50 = sorted[Math.floor(sorted.length * 0.5)];
        const p75 = sorted[Math.floor(sorted.length * 0.75)];
        const p90 = sorted[Math.floor(sorted.length * 0.9)];
        // For flash loans, we want to be in top 10% for speed
        const recommendedFee = Math.ceil(p90 * 1.1);
        // Calculate confidence based on sample size and variance
        const variance = this.calculateVariance(sorted);
        const confidence = Math.min(0.95, Math.max(0.1, (sorted.length / 100) * (1 - variance / p50)));
        this.emit('gas:optimization', {
            recommendedFee,
            confidence,
            samples: sorted.length,
            percentiles: { p50, p75, p90 }
        });
        return { recommendedFee, confidence };
    }
    calculateVariance(values) {
        if (values.length === 0)
            return 0;
        const mean = values.reduce((a, b) => a + b, 0) / values.length;
        const squaredDiffs = values.map(v => Math.pow(v - mean, 2));
        return Math.sqrt(squaredDiffs.reduce((a, b) => a + b, 0) / values.length);
    }
    dispose() {
        this.removeAllListeners();
        this.emergencyShutdown().catch(console.error);
    }
}
exports.FlashLoanOrchestrator = FlashLoanOrchestrator;
exports.default = FlashLoanOrchestrator;
//# sourceMappingURL=FlashLoanOrchestrator.js.map