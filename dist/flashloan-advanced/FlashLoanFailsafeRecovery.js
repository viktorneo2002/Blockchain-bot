"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.FlashLoanFailsafeRecovery = void 0;
const web3_js_1 = require("@solana/web3.js");
const spl_token_1 = require("@solana/spl-token");
const anchor_1 = require("@project-serum/anchor");
class FlashLoanFailsafeRecovery {
    constructor(connection, wallet, config = {}) {
        this.circuitBreakerActive = false;
        this.healthCheckInterval = null;
        this.lookupTableCache = new Map();
        this.connection = connection;
        this.wallet = wallet;
        this.config = {
            maxRetries: config.maxRetries || 5,
            retryDelayMs: config.retryDelayMs || 100,
            emergencyWallet: config.emergencyWallet || wallet.publicKey,
            priorityFeePercentile: config.priorityFeePercentile || 0.95,
            maxPriorityFeeLamports: config.maxPriorityFeeLamports || 1000000,
            circuitBreakerThreshold: config.circuitBreakerThreshold || 10,
            recoveryTimeout: config.recoveryTimeout || 30000,
            minBalanceThreshold: config.minBalanceThreshold || new anchor_1.BN(web3_js_1.LAMPORTS_PER_SOL * 0.1)
        };
        this.recoveryState = {
            isRecovering: false,
            failureCount: 0,
            lastFailureTimestamp: 0,
            recoveredPositions: new Map(),
            pendingPositions: new Map()
        };
    }
    async emergencyFundTransfer() {
        console.error('Emergency fund transfer initiated');
        // Implementation for emergency fund transfer
    }
    getPositionKey(position) {
        return `${position.tokenMint.toString()}_${position.amount.toString()}`;
    }
    async getAvailableTokensForSwap() {
        const tokenAccounts = await this.connection.getParsedTokenAccountsByOwner(this.wallet.publicKey, { programId: spl_token_1.TOKEN_PROGRAM_ID });
        return tokenAccounts.value
            .filter(account => account.account.data.parsed.info.tokenAmount.uiAmount > 0)
            .map(account => ({ mint: new web3_js_1.PublicKey(account.account.data.parsed.info.mint), amount: new anchor_1.BN(account.account.data.parsed.info.tokenAmount.amount) }));
    }
    async calculateSwapAmount(fromToken, toToken, availableAmount) {
        // Calculate optimal swap amount based on liquidity and slippage
        return new anchor_1.BN(availableAmount.toString()).muln(95).divn(100); // 95% of available amount with safety margin
    }
    async getAllTokenAccounts() {
        const accounts = await this.connection.getParsedTokenAccountsByOwner(this.wallet.publicKey, { programId: spl_token_1.TOKEN_PROGRAM_ID });
        return accounts.value;
    }
    async initializeRecoverySystem() {
        await this.validateWalletBalance();
        this.startHealthMonitoring();
        await this.loadLookupTables();
    }
    async validateWalletBalance() {
        const balance = await this.connection.getBalance(this.wallet.publicKey);
        if (new anchor_1.BN(balance).lt(this.config.minBalanceThreshold)) {
            throw new Error(`Insufficient balance for recovery operations: ${balance} lamports`);
        }
    }
    startHealthMonitoring() {
        this.healthCheckInterval = setInterval(async () => {
            try {
                await this.performHealthCheck();
            }
            catch (error) {
                console.error('Health check failed:', error);
                this.activateCircuitBreaker();
            }
        }, 5000);
    }
    async performHealthCheck() {
        const slot = await this.connection.getSlot();
        const balance = await this.connection.getBalance(this.wallet.publicKey);
        if (this.recoveryState.failureCount >= this.config.circuitBreakerThreshold) {
            this.activateCircuitBreaker();
        }
        if (new anchor_1.BN(balance).lt(this.config.minBalanceThreshold)) {
            await this.emergencyFundTransfer();
        }
    }
    activateCircuitBreaker() {
        this.circuitBreakerActive = true;
        console.error('Circuit breaker activated - halting recovery operations');
        setTimeout(() => {
            this.circuitBreakerActive = false;
            this.recoveryState.failureCount = 0;
        }, 60000);
    }
    async recoverFlashLoanPosition(position) {
        if (this.circuitBreakerActive) {
            console.error('Circuit breaker active - recovery operations halted');
            return false;
        }
        this.recoveryState.isRecovering = true;
        const positionKey = this.getPositionKey(position);
        this.recoveryState.pendingPositions.set(positionKey, position);
        try {
            const recovered = await this.executeRecoveryStrategy(position);
            if (recovered) {
                this.recoveryState.recoveredPositions.set(positionKey, position);
                this.recoveryState.pendingPositions.delete(positionKey);
                this.recoveryState.failureCount = 0;
            }
            return recovered;
        }
        catch (error) {
            console.error('Recovery failed:', error);
            this.recoveryState.failureCount++;
            this.recoveryState.lastFailureTimestamp = Date.now();
            return false;
        }
        finally {
            this.recoveryState.isRecovering = false;
        }
    }
    async executeRecoveryStrategy(position) {
        const strategies = [
            () => this.attemptDirectRepayment(position),
            () => this.attemptLiquidation(position),
            () => this.attemptEmergencySwap(position),
            () => this.attemptPartialRepayment(position),
            () => this.emergencyPositionExit(position)
        ];
        for (const strategy of strategies) {
            try {
                const success = await this.retryWithExponentialBackoff(strategy);
                if (success)
                    return true;
            }
            catch (error) {
                console.error('Strategy failed:', error);
            }
        }
        return false;
    }
    async attemptDirectRepayment(position) {
        try {
            const userTokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(position.tokenMint, this.wallet.publicKey);
            const tokenBalance = await this.getTokenBalance(userTokenAccount);
            if (tokenBalance.lt(position.amount)) {
                return false;
            }
            const instructions = [];
            instructions.push((0, spl_token_1.createTransferInstruction)(userTokenAccount, position.repaymentAccount, this.wallet.publicKey, position.amount.toNumber(), [], spl_token_1.TOKEN_PROGRAM_ID));
            const signature = await this.sendOptimizedTransaction(instructions);
            await this.confirmTransaction(signature);
            return true;
        }
        catch (error) {
            console.error('Direct repayment failed:', error);
            return false;
        }
    }
    async attemptLiquidation(position) {
        try {
            const collateralAccounts = await this.findCollateralAccounts();
            if (collateralAccounts.length === 0)
                return false;
            const instructions = [];
            let totalValue = new anchor_1.BN(0);
            for (const account of collateralAccounts) {
                const value = await this.estimateTokenValue(account.mint, account.amount);
                if (value.gt(new anchor_1.BN(0))) {
                    const liquidationIx = await this.createLiquidationInstruction(account, position.tokenMint);
                    if (liquidationIx) {
                        instructions.push(liquidationIx);
                        totalValue = totalValue.add(value);
                    }
                }
                if (totalValue.gte(position.amount))
                    break;
            }
            if (instructions.length === 0)
                return false;
            const signature = await this.sendOptimizedTransaction(instructions);
            await this.confirmTransaction(signature);
            return await this.attemptDirectRepayment(position);
        }
        catch (error) {
            console.error('Liquidation failed:', error);
            return false;
        }
    }
    async attemptEmergencySwap(position) {
        try {
            const availableTokens = await this.getAvailableTokensForSwap();
            if (availableTokens.length === 0)
                return false;
            for (const token of availableTokens) {
                const swapAmount = await this.calculateSwapAmount(token.mint, position.tokenMint, position.amount);
                if (swapAmount && token.amount.gte(swapAmount)) {
                    const swapIx = await this.createSwapInstruction(token.mint, position.tokenMint, swapAmount, position.amount);
                    if (swapIx) {
                        const signature = await this.sendOptimizedTransaction([swapIx]);
                        await this.confirmTransaction(signature);
                        return await this.attemptDirectRepayment(position);
                    }
                }
            }
            return false;
        }
        catch (error) {
            console.error('Emergency swap failed:', error);
            return false;
        }
    }
    async attemptPartialRepayment(position) {
        try {
            const userTokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(position.tokenMint, this.wallet.publicKey);
            const tokenBalance = await this.getTokenBalance(userTokenAccount);
            if (tokenBalance.eq(new anchor_1.BN(0)))
                return false;
            const repayAmount = anchor_1.BN.min(tokenBalance, position.amount);
            const instructions = [];
            instructions.push((0, spl_token_1.createTransferInstruction)(userTokenAccount, position.repaymentAccount, this.wallet.publicKey, repayAmount.toNumber(), [], spl_token_1.TOKEN_PROGRAM_ID));
            const signature = await this.sendOptimizedTransaction(instructions);
            await this.confirmTransaction(signature);
            position.amount = position.amount.sub(repayAmount);
            return position.amount.eq(new anchor_1.BN(0));
        }
        catch (error) {
            console.error('Partial repayment failed:', error);
            return false;
        }
    }
    async emergencyPositionExit(position) {
        try {
            const emergencyInstructions = [];
            const allTokenAccounts = await this.getAllTokenAccounts();
            for (const account of allTokenAccounts) {
                if (account.amount.gt(new anchor_1.BN(0))) {
                    emergencyInstructions.push((0, spl_token_1.createTransferInstruction)(account.address, await (0, spl_token_1.getAssociatedTokenAddress)(account.mint, this.config.emergencyWallet), this.wallet.publicKey, account.amount.toNumber(), [], spl_token_1.TOKEN_PROGRAM_ID));
                }
            }
            const solBalance = await this.connection.getBalance(this.wallet.publicKey);
            const minRent = await this.connection.getMinimumBalanceForRentExemption(0);
            const transferAmount = solBalance - minRent - 5000;
            if (transferAmount > 0) {
                emergencyInstructions.push(web3_js_1.SystemProgram.transfer({
                    fromPubkey: this.wallet.publicKey,
                    toPubkey: this.config.emergencyWallet,
                    lamports: transferAmount
                }));
            }
            if (emergencyInstructions.length > 0) {
                const signature = await this.sendOptimizedTransaction(emergencyInstructions);
                await this.confirmTransaction(signature);
            }
            return true;
        }
        catch (error) {
            console.error('Emergency exit failed:', error);
            return false;
        }
    }
    async retryWithExponentialBackoff(operation, maxRetries = this.config.maxRetries) {
        let lastError;
        for (let i = 0; i < maxRetries; i++) {
            try {
                return await operation();
            }
            catch (error) {
                lastError = error;
                const delay = Math.min(this.config.retryDelayMs * Math.pow(2, i), 5000);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
        throw lastError;
    }
    async sendOptimizedTransaction(instructions) {
        const priorityFee = await this.calculateOptimalPriorityFee();
        const computeUnits = this.estimateComputeUnits(instructions);
        const modifyComputeUnits = web3_js_1.ComputeBudgetProgram.setComputeUnitLimit({
            units: computeUnits
        });
        const addPriorityFee = web3_js_1.ComputeBudgetProgram.setComputeUnitPrice({
            microLamports: priorityFee
        });
        const allInstructions = [modifyComputeUnits, addPriorityFee, ...instructions];
        const { blockhash, lastValidBlockHeight } = await this.connection.getLatestBlockhash('confirmed');
        const messageV0 = new web3_js_1.Transaction();
        messageV0.recentBlockhash = blockhash;
        messageV0.feePayer = this.wallet.publicKey;
        messageV0.add(...allInstructions);
        const simulation = await this.connection.simulateTransaction(messageV0);
        if (simulation.value.err) {
            throw new Error(`Transaction simulation failed: ${JSON.stringify(simulation.value.err)}`);
        }
        messageV0.sign(this.wallet);
        const signature = await this.connection.sendRawTransaction(messageV0.serialize(), {
            skipPreflight: true,
            maxRetries: 0,
            preflightCommitment: 'confirmed'
        });
        return signature;
    }
    async confirmTransaction(signature, commitment = 'confirmed') {
        const latestBlockhash = await this.connection.getLatestBlockhash(commitment);
        const confirmation = await this.connection.confirmTransaction({
            signature,
            blockhash: latestBlockhash.blockhash,
            lastValidBlockHeight: latestBlockhash.lastValidBlockHeight
        }, commitment);
        if (confirmation.value.err) {
            throw new Error(`Transaction confirmation failed: ${confirmation.value.err}`);
        }
    }
    async calculateOptimalPriorityFee() {
        try {
            const recentPriorities = await this.connection.getRecentPrioritizationFees();
            if (recentPriorities.length === 0)
                return 1000;
            const fees = recentPriorities
                .map(item => item.prioritizationFee)
                .filter(fee => fee > 0)
                .sort((a, b) => a - b);
            if (fees.length === 0)
                return 1000;
            const percentileIndex = Math.floor(fees.length * this.config.priorityFeePercentile);
            const optimalFee = fees[percentileIndex] || fees[fees.length - 1];
            return Math.min(optimalFee, this.config.maxPriorityFeeLamports);
        }
        catch (error) {
            console.error('Failed to calculate priority fee:', error);
            return 5000;
        }
    }
    estimateComputeUnits(instructions) {
        let units = 0;
        for (const ix of instructions) {
            if (ix.programId.equals(spl_token_1.TOKEN_PROGRAM_ID)) {
                units += 3000;
            }
            else if (ix.programId.equals(spl_token_1.ASSOCIATED_TOKEN_PROGRAM_ID)) {
                units += 5000;
            }
            else if (ix.programId.equals(web3_js_1.SystemProgram.programId)) {
                units += 1000;
            }
            else {
                units += 10000;
            }
        }
        return Math.min(units * 1.2, 1400000);
    }
    async getTokenBalance(tokenAccount) {
        try {
            const account = await (0, spl_token_1.getAccount)(this.connection, tokenAccount);
            return new anchor_1.BN(account.amount.toString());
        }
        catch (error) {
            if (error instanceof spl_token_1.TokenAccountNotFoundError) {
                return new anchor_1.BN(0);
            }
            throw error;
        }
    }
    async findCollateralAccounts() {
        try {
            const tokenAccounts = await this.connection.getParsedTokenAccountsByOwner(this.wallet.publicKey, { programId: spl_token_1.TOKEN_PROGRAM_ID });
            return tokenAccounts.value
                .map(account => ({
                address: account.pubkey,
                mint: new web3_js_1.PublicKey(account.account.data.parsed.info.mint),
                amount: new anchor_1.BN(account.account.data.parsed.info.tokenAmount.amount)
            }))
                .filter(account => account.amount.gt(new anchor_1.BN(0)));
        }
        catch (error) {
            console.error('Failed to find collateral accounts:', error);
            return [];
        }
    }
    async estimateTokenValue(mint, amount) {
        try {
            const priceData = await this.fetchTokenPrice(mint);
            if (!priceData)
                return new anchor_1.BN(0);
            return amount.mul(new anchor_1.BN(Math.floor(priceData.price * 1e9))).div(new anchor_1.BN(1e9));
        }
        catch (error) {
            console.error('Failed to estimate token value:', error);
            return new anchor_1.BN(0);
        }
    }
    async fetchTokenPrice(mint) {
        try {
            // Try Pyth first
            const pythPrice = await this.fetchPythPrice(mint);
            if (pythPrice)
                return pythPrice;
            // Fallback to Switchboard
            const switchboardPrice = await this.fetchSwitchboardPrice(mint);
            if (switchboardPrice)
                return switchboardPrice;
            // Try Jupiter price API as last resort
            const jupiterPrice = await this.fetchJupiterPrice(mint);
            return jupiterPrice;
        }
        catch (error) {
            console.error('Failed to fetch token price:', error);
            return null;
        }
    }
    async fetchPythPrice(mint) {
        try {
            const pythProgramId = new web3_js_1.PublicKey('FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH');
            const priceAccountMap = {
                'So11111111111111111111111111111111111111112': 'H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG', // SOL
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 'Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD', // USDC
                'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': '3vxLXJqLqF3JG5TCbYycbKWRBbCJQLxQmBGCkyqEEefL', // USDT
                'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So': 'E4v1BBgoso9s64TQvmyownAVJbhbEPGyzA3qn4n46qj9', // mSOL
                '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs': '5mp8kbkTYwWWCsKSte8rURjTuyinsqBpJ9xAQsewPDD', // stETH
            };
            const priceAccount = priceAccountMap[mint.toString()];
            if (!priceAccount)
                return null;
            const priceAccountPubkey = new web3_js_1.PublicKey(priceAccount);
            const accountInfo = await this.connection.getAccountInfo(priceAccountPubkey);
            if (!accountInfo)
                return null;
            // Parse Pyth price data
            const priceData = parsePythPriceData(accountInfo.data);
            if (!priceData || priceData.status !== 1)
                return null;
            return {
                price: priceData.aggregate.price * Math.pow(10, priceData.exponent),
                confidence: priceData.aggregate.confidence
            };
        }
        catch (error) {
            return null;
        }
    }
    async fetchSwitchboardPrice(mint) {
        try {
            const switchboardProgramId = new web3_js_1.PublicKey('SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f');
            const aggregatorMap = {
                'So11111111111111111111111111111111111111112': 'GvDMxPzN1sCj7L26YDK2HnMRXEQmQ2aemov8YBtPS7vR', // SOL
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 'BjUgj6YCnFBZ49wF54ddBVA9qu8TeqkFtkbqmZcee8uW', // USDC
                'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': '3Dpu2kXk87mF9Ls9caWCHqyBiv2gK4mNQpPjVdZnhtVp', // USDT
            };
            const aggregatorAccount = aggregatorMap[mint.toString()];
            if (!aggregatorAccount)
                return null;
            const aggregatorPubkey = new web3_js_1.PublicKey(aggregatorAccount);
            const accountInfo = await this.connection.getAccountInfo(aggregatorPubkey);
            if (!accountInfo)
                return null;
            const aggregatorData = parseSwitchboardAggregator(accountInfo.data);
            if (!aggregatorData || !aggregatorData.latestConfirmedRound)
                return null;
            return {
                price: aggregatorData.latestConfirmedRound.result,
                confidence: aggregatorData.latestConfirmedRound.stdDeviation
            };
        }
        catch (error) {
            return null;
        }
    }
    async fetchJupiterPrice(mint) {
        try {
            const response = await fetch(`https://price.jup.ag/v4/price?ids=${mint.toString()}`);
            if (!response.ok)
                return null;
            const data = await response.json();
            const priceData = data.data[mint.toString()];
            if (!priceData)
                return null;
            return {
                price: priceData.price,
                confidence: priceData.confidence || 0.01
            };
        }
        catch (error) {
            return null;
        }
    }
    async createLiquidationInstruction(collateral, targetMint) {
        try {
            // Check if collateral is in a lending protocol
            const lendingProtocols = [
                { name: 'Solend', programId: new web3_js_1.PublicKey('So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo') },
                { name: 'Kamino', programId: new web3_js_1.PublicKey('KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD') },
                { name: 'MarginFi', programId: new web3_js_1.PublicKey('MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA') }
            ];
            for (const protocol of lendingProtocols) {
                const liquidationIx = await this.createProtocolLiquidation(protocol, collateral, targetMint);
                if (liquidationIx)
                    return liquidationIx;
            }
            // If not in lending protocol, try direct swap
            return await this.createSwapInstruction(collateral.mint, targetMint, collateral.amount, new anchor_1.BN(0));
        }
        catch (error) {
            console.error('Failed to create liquidation instruction:', error);
            return null;
        }
    }
    async createProtocolLiquidation(protocol, collateral, targetMint) {
        try {
            const data = Buffer.alloc(17);
            data.writeUInt8(0x0A, 0); // Liquidation instruction
            data.writeBigUInt64LE(BigInt(collateral.amount.toString()), 1);
            data.writeBigUInt64LE(BigInt(0), 9); // Min received
            const keys = [
                { pubkey: this.wallet.publicKey, isSigner: true, isWritable: true },
                { pubkey: collateral.address, isSigner: false, isWritable: true },
                { pubkey: await (0, spl_token_1.getAssociatedTokenAddress)(targetMint, this.wallet.publicKey), isSigner: false, isWritable: true },
                { pubkey: collateral.mint, isSigner: false, isWritable: false },
                { pubkey: targetMint, isSigner: false, isWritable: false },
                { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false }
            ];
            return new web3_js_1.TransactionInstruction({
                programId: protocol.programId,
                keys,
                data
            });
        }
        catch (error) {
            return null;
        }
    }
    async createSwapInstruction(fromMint, toMint, fromAmount, minToAmount) {
        try {
            // Try Jupiter first
            const jupiterIx = await this.createJupiterSwap(fromMint, toMint, fromAmount, minToAmount);
            if (jupiterIx)
                return jupiterIx;
            // Fallback to Raydium
            const raydiumIx = await this.createRaydiumSwap(fromMint, toMint, fromAmount, minToAmount);
            if (raydiumIx)
                return raydiumIx;
            // Try Orca
            const orcaIx = await this.createOrcaSwap(fromMint, toMint, fromAmount, minToAmount);
            return orcaIx;
        }
        catch (error) {
            console.error('Failed to create swap instruction:', error);
            return null;
        }
    }
    async createJupiterSwap(fromMint, toMint, fromAmount, minToAmount) {
        try {
            const jupiterProgramId = new web3_js_1.PublicKey('JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB');
            const quoteResponse = await fetch('https://quote-api.jup.ag/v6/quote', {
                method: 'GET',
                headers: { 'Accept': 'application/json' },
                body: new URLSearchParams({
                    inputMint: fromMint.toString(),
                    outputMint: toMint.toString(),
                    amount: fromAmount.toString(),
                    slippageBps: '100',
                    onlyDirectRoutes: 'false',
                    maxAccounts: '20'
                })
            });
            if (!quoteResponse.ok)
                return null;
            const quoteData = await quoteResponse.json();
            const swapResponse = await fetch('https://quote-api.jup.ag/v6/swap', {
                method: 'POST',
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    quoteResponse: quoteData,
                    userPublicKey: this.wallet.publicKey.toString(),
                    wrapAndUnwrapSol: true,
                    computeUnitPriceMicroLamports: 'auto',
                    dynamicComputeUnitLimit: true,
                    prioritizationFeeLamports: 'auto'
                })
            });
            if (!swapResponse.ok)
                return null;
            const swapData = await swapResponse.json();
            const swapTransaction = web3_js_1.VersionedTransaction.deserialize(Buffer.from(swapData.swapTransaction, 'base64'));
            const compiledIx = swapTransaction.message.compiledInstructions[0];
            const accountKeys = compiledIx.accountKeyIndexes.map(index => ({
                pubkey: swapTransaction.message.staticAccountKeys[index],
                isSigner: index < swapTransaction.message.header.numRequiredSignatures,
                isWritable: index < swapTransaction.message.header.numRequiredSignatures
            }));
            return new web3_js_1.TransactionInstruction({
                programId: swapTransaction.message.staticAccountKeys[compiledIx.programIdIndex],
                keys: accountKeys,
                data: Buffer.from(compiledIx.data)
            });
        }
        catch (error) {
            return null;
        }
    }
    async createRaydiumSwap(fromMint, toMint, fromAmount, minToAmount) {
        try {
            const raydiumProgramId = new web3_js_1.PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
            const raydiumAuthority = new web3_js_1.PublicKey('5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1');
            const poolKeys = await this.findRaydiumPool(fromMint, toMint);
            if (!poolKeys)
                return null;
            const userFromToken = await (0, spl_token_1.getAssociatedTokenAddress)(fromMint, this.wallet.publicKey);
            const userToToken = await (0, spl_token_1.getAssociatedTokenAddress)(toMint, this.wallet.publicKey);
            const data = Buffer.alloc(17);
            data.writeUInt8(0x09, 0); // Swap instruction
            data.writeBigUInt64LE(BigInt(fromAmount.toString()), 1);
            data.writeBigUInt64LE(BigInt(minToAmount.toString()), 9);
            const keys = [
                { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
                { pubkey: poolKeys.id, isSigner: false, isWritable: true },
                { pubkey: raydiumAuthority, isSigner: false, isWritable: false },
                { pubkey: poolKeys.openOrders, isSigner: false, isWritable: true },
                { pubkey: poolKeys.targetOrders, isSigner: false, isWritable: true },
                { pubkey: poolKeys.baseVault, isSigner: false, isWritable: true },
                { pubkey: poolKeys.quoteVault, isSigner: false, isWritable: true },
                { pubkey: poolKeys.marketProgramId, isSigner: false, isWritable: false },
                { pubkey: poolKeys.marketId, isSigner: false, isWritable: true },
                { pubkey: poolKeys.marketBids, isSigner: false, isWritable: true },
                { pubkey: poolKeys.marketAsks, isSigner: false, isWritable: true },
                { pubkey: poolKeys.marketEventQueue, isSigner: false, isWritable: true },
                { pubkey: poolKeys.marketBaseVault, isSigner: false, isWritable: true },
                { pubkey: poolKeys.marketQuoteVault, isSigner: false, isWritable: true },
                { pubkey: poolKeys.marketAuthority, isSigner: false, isWritable: false },
                { pubkey: userFromToken, isSigner: false, isWritable: true },
                { pubkey: userToToken, isSigner: false, isWritable: true },
                { pubkey: this.wallet.publicKey, isSigner: true, isWritable: false }
            ];
            return new web3_js_1.TransactionInstruction({
                programId: raydiumProgramId,
                keys,
                data
            });
        }
        catch (error) {
            return null;
        }
    }
    async createOrcaSwap(fromMint, toMint, fromAmount, minToAmount) {
        try {
            const orcaProgramId = new web3_js_1.PublicKey('9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP');
            const whirlpoolProgramId = new web3_js_1.PublicKey('whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc');
            const poolAddress = await this.findOrcaPool(fromMint, toMint);
            if (!poolAddress)
                return null;
            const userFromToken = await (0, spl_token_1.getAssociatedTokenAddress)(fromMint, this.wallet.publicKey);
            const userToToken = await (0, spl_token_1.getAssociatedTokenAddress)(toMint, this.wallet.publicKey);
            const data = Buffer.alloc(25);
            data.writeUInt8(0x02, 0); // Swap instruction
            data.writeBigUInt64LE(BigInt(fromAmount.toString()), 1);
            data.writeBigUInt64LE(BigInt(minToAmount.toString()), 9);
            data.writeBigUInt64LE(BigInt(100), 17); // Sqrt price limit
            const keys = [
                { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
                { pubkey: this.wallet.publicKey, isSigner: true, isWritable: false },
                { pubkey: poolAddress, isSigner: false, isWritable: true },
                { pubkey: userFromToken, isSigner: false, isWritable: true },
                { pubkey: userToToken, isSigner: false, isWritable: true },
                { pubkey: await this.getOrcaVault(poolAddress, 0), isSigner: false, isWritable: true },
                { pubkey: await this.getOrcaVault(poolAddress, 1), isSigner: false, isWritable: true },
                { pubkey: await this.getOrcaTickArray(poolAddress), isSigner: false, isWritable: true },
                { pubkey: await this.getOrcaOracle(poolAddress), isSigner: false, isWritable: false }
            ];
            return new web3_js_1.TransactionInstruction({
                programId: whirlpoolProgramId,
                keys,
                data
            });
        }
        catch (error) {
            return null;
        }
    }
    async findRaydiumPool(tokenA, tokenB) {
        try {
            const response = await fetch('https://api.raydium.io/v2/main/pairs');
            if (!response.ok)
                return null;
            const pairs = await response.json();
            const pair = pairs.find((p) => (p.baseMint === tokenA.toString() && p.quoteMint === tokenB.toString()) ||
                (p.baseMint === tokenB.toString() && p.quoteMint === tokenA.toString()));
            if (!pair)
                return null;
            return {
                id: new web3_js_1.PublicKey(pair.ammId),
                openOrders: new web3_js_1.PublicKey(pair.ammOpenOrders),
                targetOrders: new web3_js_1.PublicKey(pair.ammTargetOrders),
                baseVault: new web3_js_1.PublicKey(pair.baseVault),
                quoteVault: new web3_js_1.PublicKey(pair.quoteVault),
                marketProgramId: new web3_js_1.PublicKey(pair.marketProgramId),
                marketId: new web3_js_1.PublicKey(pair.marketId),
                marketBids: new web3_js_1.PublicKey(pair.marketBids),
                marketAsks: new web3_js_1.PublicKey(pair.marketAsks),
                marketEventQueue: new web3_js_1.PublicKey(pair.marketEventQueue),
                marketBaseVault: new web3_js_1.PublicKey(pair.marketBaseVault),
                marketQuoteVault: new web3_js_1.PublicKey(pair.marketQuoteVault),
                marketAuthority: new web3_js_1.PublicKey(pair.marketAuthority)
            };
        }
        catch (error) {
            return null;
        }
    }
    async findOrcaPool(tokenA, tokenB) {
        try {
            const whirlpoolProgramId = new web3_js_1.PublicKey('whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc');
            const [whirlpoolPda] = await web3_js_1.PublicKey.findProgramAddress([
                Buffer.from('whirlpool'),
                tokenA.toBuffer(),
                tokenB.toBuffer(),
                Buffer.from([250]) // Default fee tier
            ], whirlpoolProgramId);
            return whirlpoolPda;
        }
        catch (error) {
            return null;
        }
    }
    async getOrcaVault(poolAddress, index) {
        const whirlpoolProgramId = new web3_js_1.PublicKey('whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc');
        const [vault] = await web3_js_1.PublicKey.findProgramAddress([poolAddress.toBuffer(), Buffer.from([index])], whirlpoolProgramId);
        return vault;
    }
    async getOrcaTickArray(poolAddress) {
        const whirlpoolProgramId = new web3_js_1.PublicKey('whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc');
        const [tickArray] = await web3_js_1.PublicKey.findProgramAddress([Buffer.from('tick_array'), poolAddress.toBuffer(), Buffer.from([0])], whirlpoolProgramId);
        return tickArray;
    }
    async getOrcaOracle(poolAddress) {
        const whirlpoolProgramId = new web3_js_1.PublicKey('whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc');
        const [oracle] = await web3_js_1.PublicKey.findProgramAddress([Buffer.from('oracle'), poolAddress.toBuffer()], whirlpoolProgramId);
        return oracle;
    }
    async loadLookupTables() {
        try {
            const commonTables = [
                'AhhdRu5YZdjVkKR3wbnUDaymVQL4uKxVDuQ2GrqK6CTU', // Jupiter v6 Main
                '4syr5pBaboZy4cZyF6sys82uGD7jEvoAP2ZMaoich4fZ', // Raydium AMM
                'GmqbghNvgJRYJfHcUQ1YHtuWf1zZp9drgrZFZWHfRWk9', // Orca Whirlpool
                'DjF9jvuYrj6tiuvNynY8tjvx9fhqQ7fBV4gzUvhDvLMN', // Solend Main
                '11111112D1oxKts8YPdTJRG5FzxTNpMtWmq8hkVx3', // Token Metadata
                'HQ2UUt18uJqKaQFJhgV9zaTdQxUZjNrsKFgoEDquBkcx' // Serum DEX v3
            ];
            for (const tableAddress of commonTables) {
                try {
                    const lookupTable = await this.connection.getAddressLookupTable(new web3_js_1.PublicKey(tableAddress));
                    if (lookupTable.value) {
                        this.lookupTableCache.set(tableAddress, lookupTable.value);
                    }
                }
                catch (error) {
                    console.error(`Failed to load lookup table ${tableAddress}:`, error);
                }
            }
        }
        catch (error) {
            console.error('Failed to load lookup tables:', error);
        }
    }
}
exports.FlashLoanFailsafeRecovery = FlashLoanFailsafeRecovery;
function parsePythPriceData(data) {
    try {
        const magic = data.readUInt32LE(0);
        if (magic !== 0xa1b2c3d4)
            return null;
        const version = data.readUInt32LE(4);
        const type = data.readUInt32LE(8);
        const size = data.readUInt32LE(12);
        const priceType = data.readUInt32LE(16);
        const exponent = data.readInt32LE(20);
        const numComponentPrices = data.readUInt32LE(24);
        const lastSlot = data.readBigUInt64LE(32);
        const validSlot = data.readBigUInt64LE(40);
        const twap = data.readBigInt64LE(48);
        const twac = data.readBigUInt64LE(56);
        const drv1 = data.readBigInt64LE(64);
        const drv2 = data.readBigInt64LE(72);
        const productAccountKey = new web3_js_1.PublicKey(data.slice(80, 112));
        const nextPriceAccountKey = new web3_js_1.PublicKey(data.slice(112, 144));
        const previousSlot = data.readBigUInt64LE(144);
        const previousPriceComponent = data.readBigInt64LE(152);
        const previousConfidenceComponent = data.readBigUInt64LE(160);
        const previousTimestamp = data.readBigInt64LE(168);
        const aggregatePriceComponent = data.readBigInt64LE(176);
        const aggregateConfidenceComponent = data.readBigUInt64LE(184);
        const status = data.readUInt32LE(192);
        const corporateAction = data.readUInt32LE(196);
        const timestamp = data.readBigInt64LE(200);
        return {
            magic,
            version,
            type,
            size,
            priceType,
            exponent,
            numComponentPrices,
            lastSlot: Number(lastSlot),
            validSlot: Number(validSlot),
            twap: Number(twap),
            twac: Number(twac),
            productAccountKey,
            nextPriceAccountKey,
            aggregate: {
                price: Number(aggregatePriceComponent),
                confidence: Number(aggregateConfidenceComponent)
            },
            status,
            timestamp: Number(timestamp)
        };
    }
    catch (error) {
        return null;
    }
}
function parseSwitchboardAggregator(data) {
    try {
        const discriminator = data.slice(0, 8);
        const name = data.slice(8, 40).toString('utf8').replace(/\0/g, '');
        const metadata = data.slice(40, 168).toString('utf8').replace(/\0/g, '');
        const isLocked = data.readUInt8(176) === 1;
        const authority = new web3_js_1.PublicKey(data.slice(177, 209));
        const queuePubkey = new web3_js_1.PublicKey(data.slice(209, 241));
        const cranePubkey = new web3_js_1.PublicKey(data.slice(241, 273));
        const lastRoundResult = data.readDoubleLE(273);
        const lastRoundSlot = data.readBigUInt64LE(281);
        const lastRoundTimestamp = data.readBigInt64LE(289);
        const reward = data.readBigUInt64LE(297);
        const ebuf = data.slice(305, 369);
        const minUpdateDelaySeconds = data.readUInt32LE(369);
        const createdAt = data.readBigInt64LE(377);
        const startAfter = data.readBigInt64LE(385);
        const varianceThreshold = data.readDoubleLE(393);
        const forceReportPeriod = data.readBigInt64LE(401);
        const expiration = data.readBigInt64LE(409);
        const consecutiveFailureCount = data.readBigUInt64LE(417);
        const nextAllowedUpdateTime = data.readBigInt64LE(425);
        const isLocked2 = data.readUInt8(433) === 1;
        const crankPubkey = new web3_js_1.PublicKey(data.slice(434, 466));
        const latestConfirmedRoundResult = data.readDoubleLE(466);
        const latestConfirmedRoundSlot = data.readBigUInt64LE(474);
        const latestConfirmedRoundTimestamp = data.readBigInt64LE(482);
        const latestConfirmedRoundNumSuccess = data.readUInt16LE(490);
        const latestConfirmedRoundNumError = data.readUInt16LE(492);
        const latestConfirmedRoundIsClosed = data.readUInt8(494) === 1;
        const latestConfirmedRoundStdDeviation = data.readDoubleLE(495);
        const previousConfirmedRoundResult = data.readDoubleLE(503);
        const previousConfirmedRoundSlot = data.readBigUInt64LE(511);
        const previousConfirmedRoundTimestamp = data.readBigInt64LE(519);
        const previousConfirmedRoundNumSuccess = data.readUInt16LE(527);
        const previousConfirmedRoundNumError = data.readUInt16LE(529);
        const previousConfirmedRoundIsClosed = data.readUInt8(531) === 1;
        const previousConfirmedRoundStdDeviation = data.readDoubleLE(532);
        const oracleRequestBatchSize = data.readUInt32LE(540);
        const minOracleResults = data.readUInt32LE(544);
        const minJobResults = data.readUInt32LE(548);
        const historyBuffer = new web3_js_1.PublicKey(data.slice(552, 584));
        return {
            name,
            metadata,
            isLocked,
            authority,
            queuePubkey,
            lastRoundResult,
            lastRoundSlot: Number(lastRoundSlot),
            lastRoundTimestamp: Number(lastRoundTimestamp),
            latestConfirmedRound: {
                result: latestConfirmedRoundResult,
                slot: Number(latestConfirmedRoundSlot),
                timestamp: Number(latestConfirmedRoundTimestamp),
                numSuccess: latestConfirmedRoundNumSuccess,
                numError: latestConfirmedRoundNumError,
                isClosed: latestConfirmedRoundIsClosed,
                stdDeviation: latestConfirmedRoundStdDeviation
            },
            previousConfirmedRound: {
                result: previousConfirmedRoundResult,
                slot: Number(previousConfirmedRoundSlot),
                timestamp: Number(previousConfirmedRoundTimestamp),
                numSuccess: previousConfirmedRoundNumSuccess,
                numError: previousConfirmedRoundNumError,
                isClosed: previousConfirmedRoundIsClosed,
                stdDeviation: previousConfirmedRoundStdDeviation
            },
            oracleRequestBatchSize,
            minOracleResults,
            minJobResults,
            historyBuffer
        };
    }
    catch (error) {
        return null;
    }
}
//# sourceMappingURL=FlashLoanFailsafeRecovery.js.map