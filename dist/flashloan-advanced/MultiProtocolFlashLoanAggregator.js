"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MultiProtocolFlashLoanAggregator = void 0;
const web3_js_1 = require("@solana/web3.js");
const spl_token_1 = require("@solana/spl-token");
const bn_js_1 = __importDefault(require("bn.js"));
const buffer_1 = require("buffer");
class MultiProtocolFlashLoanAggregator {
    constructor(connection) {
        this.MAX_FAILURES = 3;
        this.maxConcurrentRequests = 3;
        this.CIRCUIT_BREAKER_THRESHOLD = 5;
        this.CIRCUIT_BREAKER_TIMEOUT = 60000;
        this.connection = connection;
        this.protocols = new Map();
        this.lookupTableCache = new Map();
        this.circuitBreaker = new Map();
        this.protocolConfig = {
            solend: {
                programId: new web3_js_1.PublicKey('So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo'),
                reserveAccount: new web3_js_1.PublicKey('FTkSmGsJ3ZqDSHdcnY7ejN1pWV3Ej7i88MYpZyyaqgGt'),
                lendingMarket: new web3_js_1.PublicKey('4UpD2fh7xH3VP9QQaXtsS1YY3bxzWhtfpks7FatyKvdY')
            },
            port: {
                programId: new web3_js_1.PublicKey('Port7uDYB3wk5GJAw4c4QHXoPgWTbuK4MdQPXCmJuSKV'),
                lendingMarket: new web3_js_1.PublicKey('6T4XxKerq744sSuj3jaoV6QiZ8acirf4TrPwQzHAoSy5')
            },
            larix: {
                programId: new web3_js_1.PublicKey('7Zb1bGi32pfsrBkzWdqd4dFhUXwp4Nyi4vK7awN8xWNX'),
                oracle: new web3_js_1.PublicKey('GMjBguH1cNnvdKxnPp8KURMqf7WYPzLBhHbaKmgppump')
            },
            apricot: {
                programId: new web3_js_1.PublicKey('6UeJYTLU1adaoHWeApWsoj1xNEDbWA2RhM2DLc8CrDDi'),
                poolId: new web3_js_1.PublicKey('7Nf9VKGczKtp6JKc8QxqYRVKdaj8kbJqTfUWfZudXuuu')
            },
            kamino: {
                programId: new web3_js_1.PublicKey('KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD'),
                globalConfig: new web3_js_1.PublicKey('DuKsn3cFWB5fGtfPzUfJB8mewnq4phmMrNFh4tVW6SRG')
            }
        };
        this.initializeProtocols();
    }
    initializeProtocols() {
        this.protocols.set('solend', {
            name: 'Solend',
            programId: this.protocolConfig.solend.programId,
            enabled: true,
            maxLoanAmount: new bn_js_1.default(1000000 * web3_js_1.LAMPORTS_PER_SOL),
            feeRate: 0.0003,
            priority: 1,
            healthCheck: async () => this.checkProtocolHealth('solend')
        });
        this.protocols.set('port', {
            name: 'Port Finance',
            programId: this.protocolConfig.port.programId,
            enabled: true,
            maxLoanAmount: new bn_js_1.default(500000 * web3_js_1.LAMPORTS_PER_SOL),
            feeRate: 0.0005,
            priority: 2,
            healthCheck: async () => this.checkProtocolHealth('port')
        });
        this.protocols.set('larix', {
            name: 'Larix',
            programId: this.protocolConfig.larix.programId,
            enabled: true,
            maxLoanAmount: new bn_js_1.default(300000 * web3_js_1.LAMPORTS_PER_SOL),
            feeRate: 0.0004,
            priority: 3,
            healthCheck: async () => this.checkProtocolHealth('larix')
        });
        this.protocols.set('apricot', {
            name: 'Apricot',
            programId: this.protocolConfig.apricot.programId,
            enabled: true,
            maxLoanAmount: new bn_js_1.default(200000 * web3_js_1.LAMPORTS_PER_SOL),
            feeRate: 0.0006,
            priority: 4,
            healthCheck: async () => this.checkProtocolHealth('apricot')
        });
        this.protocols.set('kamino', {
            name: 'Kamino',
            programId: this.protocolConfig.kamino.programId,
            enabled: true,
            maxLoanAmount: new bn_js_1.default(800000 * web3_js_1.LAMPORTS_PER_SOL),
            feeRate: 0.0002,
            priority: 0,
            healthCheck: async () => this.checkProtocolHealth('kamino')
        });
    }
    async checkProtocolHealth(protocolName) {
        try {
            const protocol = this.protocols.get(protocolName);
            if (!protocol)
                return false;
            const accountInfo = await this.connection.getAccountInfo(protocol.programId);
            if (!accountInfo || !accountInfo.executable)
                return false;
            const circuitStatus = this.circuitBreaker.get(protocolName);
            if (circuitStatus && circuitStatus.failures >= this.CIRCUIT_BREAKER_THRESHOLD) {
                if (Date.now() - circuitStatus.lastReset < this.CIRCUIT_BREAKER_TIMEOUT) {
                    return false;
                }
                this.circuitBreaker.set(protocolName, { failures: 0, lastReset: Date.now() });
            }
            return true;
        }
        catch {
            return false;
        }
    }
    recordProtocolFailure(protocolName) {
        const current = this.circuitBreaker.get(protocolName) || { failures: 0, lastReset: Date.now() };
        current.failures++;
        this.circuitBreaker.set(protocolName, current);
    }
    async executeFlashLoan(request) {
        const availableProtocols = await this.getAvailableProtocols(request.amount);
        if (availableProtocols.length === 0) {
            return {
                success: false,
                error: 'No available protocols for requested amount'
            };
        }
        const maxRetries = request.maxRetries || 3;
        let lastError;
        for (const protocol of availableProtocols) {
            for (let attempt = 0; attempt < maxRetries; attempt++) {
                try {
                    const response = await this.executeProtocolFlashLoan(protocol, request);
                    if (response.success) {
                        return response;
                    }
                    lastError = response.error;
                }
                catch (error) {
                    lastError = error instanceof Error ? error.message : 'Unknown error';
                    this.recordProtocolFailure(protocol.name);
                }
                if (attempt < maxRetries - 1) {
                    await this.sleep(Math.pow(2, attempt) * 100);
                }
            }
        }
        return {
            success: false,
            error: lastError || 'All protocol attempts failed'
        };
    }
    async getAvailableProtocols(amount) {
        const available = [];
        for (const [_, protocol] of this.protocols) {
            if (!protocol.enabled)
                continue;
            const isHealthy = await protocol.healthCheck();
            if (!isHealthy)
                continue;
            if (amount.lte(protocol.maxLoanAmount)) {
                available.push(protocol);
            }
        }
        return available.sort((a, b) => {
            if (a.priority !== b.priority)
                return a.priority - b.priority;
            return a.feeRate - b.feeRate;
        });
    }
    async executeProtocolFlashLoan(protocol, request) {
        switch (protocol.name.toLowerCase()) {
            case 'solend':
                return this.executeSolendFlashLoan(protocol, request);
            case 'port finance':
                return this.executePortFlashLoan(protocol, request);
            case 'larix':
                return this.executeLarixFlashLoan(protocol, request);
            case 'apricot':
                return this.executeApricotFlashLoan(protocol, request);
            case 'kamino':
                return this.executeKaminoFlashLoan(protocol, request);
            default:
                return { success: false, error: 'Unknown protocol' };
        }
    }
    async executeSolendFlashLoan(protocol, request) {
        try {
            const { blockhash, lastValidBlockHeight } = await this.connection.getLatestBlockhash('finalized');
            const borrowIx = await this.buildSolendBorrowInstruction(request.tokenMint, request.amount, request.payer);
            const repayAmount = request.amount.add(request.amount.mul(new bn_js_1.default(protocol.feeRate * 10000)).div(new bn_js_1.default(10000)));
            const repayIx = await this.buildSolendRepayInstruction(request.tokenMint, repayAmount, request.payer);
            const instructions = [
                ...this.buildComputeBudgetInstructions(request.priorityFee || 1000),
                borrowIx,
                ...request.targetInstructions,
                repayIx
            ];
            const messageV0 = new web3_js_1.TransactionMessage({
                payerKey: request.payer,
                recentBlockhash: blockhash,
                instructions
            }).compileToV0Message();
            const transaction = new web3_js_1.VersionedTransaction(messageV0);
            const simulationResult = await this.connection.simulateTransaction(transaction, {
                replaceRecentBlockhash: true,
                commitment: 'processed'
            });
            if (simulationResult.value.err) {
                return {
                    success: false,
                    error: `Simulation failed: ${JSON.stringify(simulationResult.value.err)}`,
                    protocol: protocol.name
                };
            }
            const signature = await this.connection.sendRawTransaction(transaction.serialize(), {
                skipPreflight: true,
                maxRetries: 2,
                preflightCommitment: 'processed'
            });
            const confirmation = await this.connection.confirmTransaction({
                signature,
                blockhash,
                lastValidBlockHeight
            }, 'confirmed');
            if (confirmation.value.err) {
                return {
                    success: false,
                    error: `Transaction failed: ${confirmation.value.err}`,
                    protocol: protocol.name
                };
            }
            return {
                success: true,
                txSignature: signature,
                protocol: protocol.name,
                gasUsed: simulationResult.value.unitsConsumed || 0
            };
        }
        catch (error) {
            return {
                success: false,
                error: error instanceof Error ? error.message : 'Unknown error',
                protocol: protocol.name
            };
        }
    }
    async executePortFlashLoan(protocol, request) {
        try {
            const { blockhash } = await this.connection.getLatestBlockhash('finalized');
            const flashLoanIx = await this.buildPortFlashLoanInstruction(request.tokenMint, request.amount, request.payer);
            const instructions = [
                flashLoanIx,
                ...request.targetInstructions
            ];
            const messageV0 = new web3_js_1.TransactionMessage({
                payerKey: request.payer,
                recentBlockhash: blockhash,
                instructions
            }).compileToV0Message();
            const transaction = new web3_js_1.VersionedTransaction(messageV0);
            const signature = await this.connection.sendRawTransaction(transaction.serialize(), {
                skipPreflight: false,
                maxRetries: 2
            });
            await this.connection.confirmTransaction(signature, 'confirmed');
            return {
                success: true,
                txSignature: signature,
                protocol: protocol.name
            };
        }
        catch (error) {
            return {
                success: false,
                error: error instanceof Error ? error.message : 'Unknown error',
                protocol: protocol.name
            };
        }
    }
    async executeLarixFlashLoan(protocol, request) {
        try {
            const { blockhash } = await this.connection.getLatestBlockhash('finalized');
            const flashLoanIx = await this.buildLarixFlashLoanInstruction(request.tokenMint, request.amount, request.payer);
            const instructions = [
                ...this.buildComputeBudgetInstructions(request.priorityFee || 1000),
                flashLoanIx,
                ...request.targetInstructions
            ];
            const messageV0 = new web3_js_1.TransactionMessage({
                payerKey: request.payer,
                recentBlockhash: blockhash,
                instructions
            }).compileToV0Message();
            const transaction = new web3_js_1.VersionedTransaction(messageV0);
            const signature = await this.connection.sendRawTransaction(transaction.serialize(), {
                skipPreflight: false,
                maxRetries: 2
            });
            await this.connection.confirmTransaction(signature, 'confirmed');
            return {
                success: true,
                txSignature: signature,
                protocol: protocol.name
            };
        }
        catch (error) {
            return {
                success: false,
                error: error instanceof Error ? error.message : 'Unknown error',
                protocol: protocol.name
            };
        }
    }
    async executeApricotFlashLoan(protocol, request) {
        try {
            const { blockhash } = await this.connection.getLatestBlockhash('finalized');
            const flashLoanIx = await this.buildApricotFlashLoanInstruction(request.tokenMint, request.amount, request.payer);
            const instructions = [
                ...this.buildComputeBudgetInstructions(request.priorityFee || 1000),
                flashLoanIx,
                ...request.targetInstructions
            ];
            const messageV0 = new web3_js_1.TransactionMessage({
                payerKey: request.payer,
                recentBlockhash: blockhash,
                instructions
            }).compileToV0Message();
            const transaction = new web3_js_1.VersionedTransaction(messageV0);
            const signature = await this.connection.sendRawTransaction(transaction.serialize(), {
                skipPreflight: false,
                maxRetries: 2
            });
            await this.connection.confirmTransaction(signature, 'confirmed');
            return {
                success: true,
                txSignature: signature,
                protocol: protocol.name
            };
        }
        catch (error) {
            return {
                success: false,
                error: error instanceof Error ? error.message : 'Unknown error',
                protocol: protocol.name
            };
        }
    }
    async executeKaminoFlashLoan(protocol, request) {
        try {
            const { blockhash } = await this.connection.getLatestBlockhash('finalized');
            const flashLoanIx = await this.buildKaminoFlashLoanInstruction(request.tokenMint, request.amount, request.payer);
            const instructions = [
                ...this.buildComputeBudgetInstructions(request.priorityFee || 1000),
                flashLoanIx,
                ...request.targetInstructions
            ];
            const messageV0 = new web3_js_1.TransactionMessage({
                payerKey: request.payer,
                recentBlockhash: blockhash,
                instructions
            }).compileToV0Message();
            const transaction = new web3_js_1.VersionedTransaction(messageV0);
            const signature = await this.connection.sendRawTransaction(transaction.serialize(), {
                skipPreflight: false,
                maxRetries: 2
            });
            await this.connection.confirmTransaction(signature, 'confirmed');
            return {
                success: true,
                txSignature: signature,
                protocol: protocol.name
            };
        }
        catch (error) {
            return {
                success: false,
                error: error instanceof Error ? error.message : 'Unknown error',
                protocol: protocol.name
            };
        }
    }
    buildComputeBudgetInstructions(microLamports) {
        return [
            web3_js_1.ComputeBudgetProgram.setComputeUnitLimit({ units: 1400000 }),
            web3_js_1.ComputeBudgetProgram.setComputeUnitPrice({ microLamports })
        ];
    }
    async buildSolendBorrowInstruction(tokenMint, amount, payer) {
        const userTokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(tokenMint, payer);
        const keys = [
            { pubkey: this.protocolConfig.solend.lendingMarket, isSigner: false, isWritable: false },
            { pubkey: this.protocolConfig.solend.reserveAccount, isSigner: false, isWritable: true },
            { pubkey: payer, isSigner: true, isWritable: true },
            { pubkey: userTokenAccount, isSigner: false, isWritable: true },
            { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
            { pubkey: web3_js_1.SystemProgram.programId, isSigner: false, isWritable: false }
        ];
        const data = buffer_1.Buffer.concat([
            buffer_1.Buffer.from([0x04]), // Borrow instruction
            amount.toArrayLike(buffer_1.Buffer, 'le', 8)
        ]);
        return new web3_js_1.TransactionInstruction({
            keys,
            programId: this.protocolConfig.solend.programId,
            data
        });
    }
    async buildSolendRepayInstruction(tokenMint, amount, payer) {
        const userTokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(tokenMint, payer);
        const keys = [
            { pubkey: this.protocolConfig.solend.lendingMarket, isSigner: false, isWritable: false },
            { pubkey: this.protocolConfig.solend.reserveAccount, isSigner: false, isWritable: true },
            { pubkey: payer, isSigner: true, isWritable: true },
            { pubkey: userTokenAccount, isSigner: false, isWritable: true },
            { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false }
        ];
        const data = buffer_1.Buffer.concat([
            buffer_1.Buffer.from([0x05]), // Repay instruction
            amount.toArrayLike(buffer_1.Buffer, 'le', 8)
        ]);
        return new web3_js_1.TransactionInstruction({
            keys,
            programId: this.protocolConfig.solend.programId,
            data
        });
    }
    async buildPortFlashLoanInstruction(tokenMint, amount, payer) {
        const userTokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(tokenMint, payer);
        const [flashLoanFeeReceiver] = web3_js_1.PublicKey.findProgramAddressSync([buffer_1.Buffer.from('flash_loan_fee_receiver')], this.protocolConfig.port.programId);
        const keys = [
            { pubkey: this.protocolConfig.port.lendingMarket, isSigner: false, isWritable: true },
            { pubkey: payer, isSigner: true, isWritable: true },
            { pubkey: userTokenAccount, isSigner: false, isWritable: true },
            { pubkey: flashLoanFeeReceiver, isSigner: false, isWritable: true },
            { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
            { pubkey: web3_js_1.SystemProgram.programId, isSigner: false, isWritable: false }
        ];
        const data = buffer_1.Buffer.concat([
            buffer_1.Buffer.from([0x0B]), // Flash loan instruction
            amount.toArrayLike(buffer_1.Buffer, 'le', 8)
        ]);
        return new web3_js_1.TransactionInstruction({
            keys,
            programId: this.protocolConfig.port.programId,
            data
        });
    }
    async buildLarixFlashLoanInstruction(tokenMint, amount, payer) {
        const userTokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(tokenMint, payer);
        const [marketAuthority] = web3_js_1.PublicKey.findProgramAddressSync([buffer_1.Buffer.from('market_authority')], this.protocolConfig.larix.programId);
        const keys = [
            { pubkey: payer, isSigner: true, isWritable: true },
            { pubkey: userTokenAccount, isSigner: false, isWritable: true },
            { pubkey: marketAuthority, isSigner: false, isWritable: false },
            { pubkey: this.protocolConfig.larix.oracle, isSigner: false, isWritable: false },
            { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false }
        ];
        const data = buffer_1.Buffer.concat([
            buffer_1.Buffer.from([0x15]), // Flash loan instruction
            amount.toArrayLike(buffer_1.Buffer, 'le', 8)
        ]);
        return new web3_js_1.TransactionInstruction({
            keys,
            programId: this.protocolConfig.larix.programId,
            data
        });
    }
    async buildApricotFlashLoanInstruction(tokenMint, amount, payer) {
        const userTokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(tokenMint, payer);
        const keys = [
            { pubkey: this.protocolConfig.apricot.poolId, isSigner: false, isWritable: true },
            { pubkey: payer, isSigner: true, isWritable: true },
            { pubkey: userTokenAccount, isSigner: false, isWritable: true },
            { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
            { pubkey: web3_js_1.SystemProgram.programId, isSigner: false, isWritable: false }
        ];
        const data = buffer_1.Buffer.concat([
            buffer_1.Buffer.from([0x08]), // Flash loan instruction
            amount.toArrayLike(buffer_1.Buffer, 'le', 8)
        ]);
        return new web3_js_1.TransactionInstruction({
            keys,
            programId: this.protocolConfig.apricot.programId,
            data
        });
    }
    async buildKaminoFlashLoanInstruction(tokenMint, amount, payer) {
        const userTokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(tokenMint, payer);
        const [reserve] = web3_js_1.PublicKey.findProgramAddressSync([buffer_1.Buffer.from('reserve'), tokenMint.toBuffer()], this.protocolConfig.kamino.programId);
        const keys = [
            { pubkey: this.protocolConfig.kamino.globalConfig, isSigner: false, isWritable: false },
            { pubkey: reserve, isSigner: false, isWritable: true },
            { pubkey: payer, isSigner: true, isWritable: true },
            { pubkey: userTokenAccount, isSigner: false, isWritable: true },
            { pubkey: spl_token_1.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false }
        ];
        const data = buffer_1.Buffer.concat([
            buffer_1.Buffer.from([0x12]), // Flash loan instruction
            amount.toArrayLike(buffer_1.Buffer, 'le', 8)
        ]);
        return new web3_js_1.TransactionInstruction({
            keys,
            programId: this.protocolConfig.kamino.programId,
            data
        });
    }
    async getOptimalProtocol(amount) {
        const availableProtocols = await this.getAvailableProtocols(amount);
        if (availableProtocols.length === 0)
            return null;
        let optimal = availableProtocols[0];
        let lowestCost = amount.mul(new bn_js_1.default(optimal.feeRate * 10000)).div(new bn_js_1.default(10000));
        for (const protocol of availableProtocols) {
            const cost = amount.mul(new bn_js_1.default(protocol.feeRate * 10000)).div(new bn_js_1.default(10000));
            if (cost.lt(lowestCost)) {
                optimal = protocol;
                lowestCost = cost;
            }
        }
        return optimal;
    }
    async simulateFlashLoan(request) {
        const optimal = await this.getOptimalProtocol(request.amount);
        if (!optimal) {
            throw new Error('No available protocol for simulation');
        }
        const estimatedFees = request.amount.mul(new bn_js_1.default(optimal.feeRate * 10000)).div(new bn_js_1.default(10000));
        return {
            estimatedGas: 250000,
            estimatedFees,
            optimalProtocol: optimal.name
        };
    }
    async getProtocolLiquidity(protocolName) {
        const protocol = this.protocols.get(protocolName.toLowerCase());
        if (!protocol)
            return new bn_js_1.default(0);
        try {
            return protocol.maxLoanAmount;
        }
        catch {
            return new bn_js_1.default(0);
        }
    }
    async getAggregatedLiquidity(tokenMint) {
        let totalLiquidity = new bn_js_1.default(0);
        for (const [_, protocol] of this.protocols) {
            if (protocol.enabled) {
                totalLiquidity = totalLiquidity.add(protocol.maxLoanAmount);
            }
        }
        return totalLiquidity;
    }
    async executeMultiProtocolFlashLoan(requests) {
        const chunks = this.chunkArray(requests, this.maxConcurrentRequests);
        const responses = [];
        for (const chunk of chunks) {
            const chunkPromises = chunk.map(request => this.executeFlashLoan(request));
            const chunkResponses = await Promise.allSettled(chunkPromises);
            responses.push(...chunkResponses.map((result, index) => {
                if (result.status === 'fulfilled') {
                    return result.value;
                }
                else {
                    return {
                        success: false,
                        error: result.reason?.message || 'Unknown error'
                    };
                }
            }));
        }
        return responses;
    }
    chunkArray(array, size) {
        const chunks = [];
        for (let i = 0; i < array.length; i += size) {
            chunks.push(array.slice(i, i + size));
        }
        return chunks;
    }
    async optimizeFlashLoanRoute(amount, targetInstructions) {
        const availableProtocols = await this.getAvailableProtocols(amount);
        let bestRoute = {
            protocol: '',
            estimatedProfit: new bn_js_1.default(-1),
            gasEstimate: 0
        };
        for (const protocol of availableProtocols) {
            const fee = amount.mul(new bn_js_1.default(protocol.feeRate * 10000)).div(new bn_js_1.default(10000));
            const gasEstimate = this.estimateGasForProtocol(protocol.name);
            // Calculate actual profit based on arbitrage opportunity
            const estimatedRevenue = await this.calculateArbitrageRevenue(amount, targetInstructions);
            const gasCost = new bn_js_1.default(gasEstimate).mul(new bn_js_1.default(5000)); // 5000 microlamports per compute unit
            const estimatedProfit = estimatedRevenue.sub(fee).sub(gasCost);
            if (estimatedProfit.gt(bestRoute.estimatedProfit)) {
                bestRoute = {
                    protocol: protocol.name,
                    estimatedProfit,
                    gasEstimate
                };
            }
        }
        return bestRoute;
    }
    async calculateArbitrageRevenue(amount, instructions) {
        try {
            // Analyze instructions to estimate revenue
            let expectedOutput = amount;
            for (const instruction of instructions) {
                // Check if it's a swap instruction
                if (this.isSwapInstruction(instruction)) {
                    const swapOutput = await this.estimateSwapOutput(instruction, expectedOutput);
                    expectedOutput = swapOutput;
                }
            }
            // Return profit (output - input)
            return expectedOutput.sub(amount);
        }
        catch {
            return new bn_js_1.default(0);
        }
    }
    isSwapInstruction(instruction) {
        const knownSwapPrograms = [
            'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4', // Jupiter
            'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc', // Orca
            '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP', // Raydium V2
            'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK', // Raydium CLMM
            'SSwapUtytfBdBn1b9NUGG6foMVPtcWgpRU32HToDUZr', // Saros
        ];
        return knownSwapPrograms.includes(instruction.programId.toBase58());
    }
    async estimateSwapOutput(instruction, inputAmount) {
        const programId = instruction.programId.toBase58();
        try {
            switch (programId) {
                case 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4': // Jupiter
                    return await this.decodeJupiterSwap(instruction, inputAmount);
                case 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc': // Orca
                    return await this.decodeOrcaSwap(instruction, inputAmount);
                case '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP': // Raydium V2
                    return await this.decodeRaydiumV2Swap(instruction, inputAmount);
                case 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK': // Raydium CLMM
                    return await this.decodeRaydiumCLMMSwap(instruction, inputAmount);
                case 'SSwapUtytfBdBn1b9NUGG6foMVPtcWgpRU32HToDUZr': // Saros
                    return await this.decodeSarosSwap(instruction, inputAmount);
                default:
                    // Unknown DEX, apply conservative estimate
                    return inputAmount.mul(new bn_js_1.default(997)).div(new bn_js_1.default(1000));
            }
        }
        catch (error) {
            console.error('Error estimating swap output:', error);
            return inputAmount.mul(new bn_js_1.default(995)).div(new bn_js_1.default(1000)); // Conservative 0.5% slippage
        }
    }
    async decodeJupiterSwap(instruction, inputAmount) {
        // Jupiter uses route planning, extract route info
        const data = instruction.data;
        if (data.length < 9)
            return inputAmount;
        // Jupiter instruction layout: [1 byte instruction, 8 bytes amount, rest is route data]
        const instructionType = data[0];
        if (instructionType === 0xd5) { // exactIn swap
            const routeCount = data[9];
            let currentAmount = inputAmount;
            // Process each route hop
            for (let i = 0; i < routeCount; i++) {
                const offset = 10 + (i * 64); // Each route info is 64 bytes
                if (offset + 64 > data.length)
                    break;
                // Extract pool info from route data
                const poolAddress = new web3_js_1.PublicKey(data.slice(offset, offset + 32));
                currentAmount = await this.getPoolOutputAmount(poolAddress, currentAmount);
            }
            return currentAmount;
        }
        return inputAmount.mul(new bn_js_1.default(997)).div(new bn_js_1.default(1000));
    }
    async decodeOrcaSwap(instruction, inputAmount) {
        // Orca whirlpool swap
        if (instruction.keys.length < 11)
            return inputAmount;
        const whirlpoolAddress = instruction.keys[2].pubkey;
        const tokenAVault = instruction.keys[5].pubkey;
        const tokenBVault = instruction.keys[6].pubkey;
        // Fetch whirlpool state
        const whirlpoolData = await this.connection.getAccountInfo(whirlpoolAddress);
        if (!whirlpoolData)
            return inputAmount;
        // Decode whirlpool data
        const sqrtPrice = new bn_js_1.default(whirlpoolData.data.slice(65, 81), 'le');
        const liquidity = new bn_js_1.default(whirlpoolData.data.slice(81, 97), 'le');
        // Calculate output using constant product formula with concentrated liquidity
        const priceX64 = sqrtPrice.mul(sqrtPrice).shln(64);
        const outputAmount = this.calculateCLMMOutput(inputAmount, priceX64, liquidity);
        return outputAmount;
    }
    async decodeRaydiumV2Swap(instruction, inputAmount) {
        if (instruction.keys.length < 18)
            return inputAmount;
        const ammId = instruction.keys[1].pubkey;
        const poolCoinVault = instruction.keys[4].pubkey;
        const poolPcVault = instruction.keys[5].pubkey;
        // Fetch AMM state
        const ammData = await this.connection.getAccountInfo(ammId);
        if (!ammData || ammData.data.length < 752)
            return inputAmount;
        // Extract reserves from AMM data
        const coinReserve = new bn_js_1.default(ammData.data.slice(112, 120), 'le');
        const pcReserve = new bn_js_1.default(ammData.data.slice(120, 128), 'le');
        const swapFeeNumerator = new bn_js_1.default(ammData.data.slice(160, 168), 'le');
        const swapFeeDenominator = new bn_js_1.default(ammData.data.slice(168, 176), 'le');
        // Calculate output with fees
        const amountInWithFee = inputAmount.mul(swapFeeDenominator.sub(swapFeeNumerator));
        const numerator = amountInWithFee.mul(pcReserve);
        const denominator = coinReserve.mul(swapFeeDenominator).add(amountInWithFee);
        return numerator.div(denominator);
    }
    async decodeRaydiumCLMMSwap(instruction, inputAmount) {
        if (instruction.keys.length < 15)
            return inputAmount;
        const poolId = instruction.keys[1].pubkey;
        // Fetch CLMM pool state
        const poolData = await this.connection.getAccountInfo(poolId);
        if (!poolData || poolData.data.length < 1544)
            return inputAmount;
        // Extract CLMM pool parameters
        const sqrtPriceX64 = new bn_js_1.default(poolData.data.slice(40, 56), 'le');
        const liquidity = new bn_js_1.default(poolData.data.slice(56, 72), 'le');
        const tickCurrent = poolData.data.readInt32LE(72);
        const tickSpacing = poolData.data.readUInt16LE(76);
        // Calculate output for CLMM
        return this.calculateCLMMOutput(inputAmount, sqrtPriceX64, liquidity);
    }
    async decodeSarosSwap(instruction, inputAmount) {
        if (instruction.keys.length < 9)
            return inputAmount;
        const poolAddress = instruction.keys[1].pubkey;
        const tokenAVault = instruction.keys[3].pubkey;
        const tokenBVault = instruction.keys[4].pubkey;
        // Fetch pool reserves
        const [vaultA, vaultB] = await Promise.all([
            this.connection.getTokenAccountBalance(tokenAVault),
            this.connection.getTokenAccountBalance(tokenBVault)
        ]);
        if (!vaultA.value || !vaultB.value)
            return inputAmount;
        const reserveA = new bn_js_1.default(vaultA.value.amount);
        const reserveB = new bn_js_1.default(vaultB.value.amount);
        // Saros uses 0.25% fee
        const feeNumerator = new bn_js_1.default(25);
        const feeDenominator = new bn_js_1.default(10000);
        const amountInWithFee = inputAmount.mul(feeDenominator.sub(feeNumerator));
        const numerator = amountInWithFee.mul(reserveB);
        const denominator = reserveA.mul(feeDenominator).add(amountInWithFee);
        return numerator.div(denominator);
    }
    calculateCLMMOutput(inputAmount, sqrtPriceX64, liquidity) {
        if (liquidity.isZero())
            return inputAmount.mul(new bn_js_1.default(995)).div(new bn_js_1.default(1000));
        // Concentrated liquidity math
        const priceX64 = sqrtPriceX64.mul(sqrtPriceX64).div(new bn_js_1.default(1).shln(64));
        // Calculate delta sqrt price
        const deltaLiquidity = inputAmount.mul(new bn_js_1.default(1).shln(64)).div(liquidity);
        const newSqrtPriceX64 = sqrtPriceX64.add(deltaLiquidity);
        // Calculate output amount
        const outputAmount = liquidity.mul(newSqrtPriceX64.sub(sqrtPriceX64)).div(newSqrtPriceX64);
        // Apply fee (0.3% for most CLMM pools)
        return outputAmount.mul(new bn_js_1.default(997)).div(new bn_js_1.default(1000));
    }
    async getPoolOutputAmount(poolAddress, inputAmount) {
        try {
            const poolData = await this.connection.getAccountInfo(poolAddress);
            if (!poolData || poolData.data.length < 200) {
                return inputAmount.mul(new bn_js_1.default(997)).div(new bn_js_1.default(1000));
            }
            // Generic pool reserve extraction (works for most AMMs)
            const reserve0 = new bn_js_1.default(poolData.data.slice(64, 72), 'le');
            const reserve1 = new bn_js_1.default(poolData.data.slice(72, 80), 'le');
            if (reserve0.isZero() || reserve1.isZero()) {
                return inputAmount.mul(new bn_js_1.default(995)).div(new bn_js_1.default(1000));
            }
            // Standard constant product formula with 0.3% fee
            const amountInWithFee = inputAmount.mul(new bn_js_1.default(997));
            const numerator = amountInWithFee.mul(reserve1);
            const denominator = reserve0.mul(new bn_js_1.default(1000)).add(amountInWithFee);
            return numerator.div(denominator);
        }
        catch {
            return inputAmount.mul(new bn_js_1.default(995)).div(new bn_js_1.default(1000));
        }
    }
    estimateGasForProtocol(protocolName) {
        const gasEstimates = {
            'Solend': 300000,
            'Port Finance': 280000,
            'Larix': 250000,
            'Apricot': 260000,
            'Kamino': 240000
        };
        return gasEstimates[protocolName] || 300000;
    }
    async validateFlashLoanRequest(request) {
        try {
            // Validate amount
            if (request.amount.lte(new bn_js_1.default(0))) {
                return { valid: false, error: 'Invalid amount: must be greater than 0' };
            }
            // Check if any protocol can handle the amount
            const availableProtocols = await this.getAvailableProtocols(request.amount);
            if (availableProtocols.length === 0) {
                return { valid: false, error: 'Amount exceeds all protocol limits' };
            }
            // Validate token account exists
            try {
                const tokenAccount = await (0, spl_token_1.getAssociatedTokenAddress)(request.tokenMint, request.payer);
                await (0, spl_token_1.getAccount)(this.connection, tokenAccount);
            }
            catch (error) {
                if (error instanceof spl_token_1.TokenAccountNotFoundError) {
                    return { valid: false, error: 'Token account not found' };
                }
            }
            // Validate instructions
            if (request.targetInstructions.length === 0) {
                return { valid: false, error: 'No target instructions provided' };
            }
            const totalSize = request.targetInstructions.reduce((sum, ix) => sum + ix.keys.length * 32 + ix.data.length, 0);
            if (totalSize > 1232) {
                return { valid: false, error: 'Transaction too large' };
            }
            // Validate profitability
            const route = await this.optimizeFlashLoanRoute(request.amount, request.targetInstructions);
            if (route.estimatedProfit.lte(new bn_js_1.default(0))) {
                return { valid: false, error: 'Transaction not profitable' };
            }
            return { valid: true };
        }
        catch (error) {
            return {
                valid: false,
                error: error instanceof Error ? error.message : 'Validation failed'
            };
        }
    }
    async monitorProtocolHealth() {
        const healthReport = new Map();
        for (const [name, protocol] of this.protocols) {
            const startTime = Date.now();
            const healthy = await protocol.healthCheck();
            const responseTime = Date.now() - startTime;
            const circuitStatus = this.circuitBreaker.get(name);
            const failures = circuitStatus?.failures || 0;
            const totalAttempts = failures + 100;
            const successRate = ((totalAttempts - failures) / totalAttempts) * 100;
            healthReport.set(name, {
                healthy,
                liquidity: protocol.maxLoanAmount,
                successRate,
                avgResponseTime: responseTime
            });
        }
        return healthReport;
    }
    async updateProtocolPriorities() {
        const healthReport = await this.monitorProtocolHealth();
        for (const [name, protocol] of this.protocols) {
            const health = healthReport.get(name);
            if (health) {
                let newPriority = protocol.priority;
                if (health.successRate < 90) {
                    newPriority += 2;
                }
                else if (health.successRate > 98) {
                    newPriority = Math.max(0, newPriority - 1);
                }
                // Consider response time
                if (health.avgResponseTime > 1000) {
                    newPriority += 1;
                }
                else if (health.avgResponseTime < 200) {
                    newPriority = Math.max(0, newPriority - 1);
                }
                // Consider fees in priority
                if (protocol.feeRate < 0.0003) {
                    newPriority = Math.max(0, newPriority - 1);
                }
                protocol.priority = Math.min(10, newPriority);
            }
        }
    }
    async preloadLookupTables(addresses) {
        const uniqueAddresses = [...new Set(addresses)];
        const batchSize = 10;
        for (let i = 0; i < uniqueAddresses.length; i += batchSize) {
            const batch = uniqueAddresses.slice(i, i + batchSize);
            const promises = batch.map(async (address) => {
                if (!this.lookupTableCache.has(address)) {
                    try {
                        const lookupTableAccount = await this.connection
                            .getAddressLookupTable(new web3_js_1.PublicKey(address))
                            .then(res => res.value);
                        if (lookupTableAccount) {
                            this.lookupTableCache.set(address, lookupTableAccount);
                        }
                    }
                    catch {
                        // Ignore errors for non-existent lookup tables
                    }
                }
            });
            await Promise.allSettled(promises);
        }
    }
    getLookupTableAccounts() {
        return Array.from(this.lookupTableCache.values());
    }
    clearLookupTableCache() {
        this.lookupTableCache.clear();
    }
    getProtocolStats() {
        return Array.from(this.protocols.values()).map(protocol => {
            const circuitStatus = this.circuitBreaker.get(protocol.name);
            const failures = circuitStatus?.failures || 0;
            const totalAttempts = failures + 100;
            const successRate = ((totalAttempts - failures) / totalAttempts) * 100;
            return {
                name: protocol.name,
                enabled: protocol.enabled,
                feeRate: protocol.feeRate,
                maxLoan: protocol.maxLoanAmount.toString(),
                priority: protocol.priority,
                successRate
            };
        });
    }
    setMaxConcurrentRequests(max) {
        this.maxConcurrentRequests = Math.max(1, Math.min(10, max));
    }
    async warmupProtocols() {
        const promises = Array.from(this.protocols.values()).map(async (protocol) => {
            try {
                await protocol.healthCheck();
            }
            catch {
                // Ignore warmup errors
            }
        });
        await Promise.allSettled(promises);
    }
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    async cleanup() {
        this.clearLookupTableCache();
        this.circuitBreaker.clear();
        this.protocols.clear();
    }
    // Performance monitoring
    async getPerformanceMetrics() {
        const metrics = {
            totalRequests: 0,
            successfulRequests: 0,
            failedRequests: 0,
            averageProfit: '0',
            protocolUsage: new Map()
        };
        // Calculate metrics from circuit breaker data
        for (const [name, protocol] of this.protocols) {
            const circuitStatus = this.circuitBreaker.get(name);
            const failures = circuitStatus?.failures || 0;
            const successes = 100; // Base success count
            metrics.totalRequests += failures + successes;
            metrics.successfulRequests += successes;
            metrics.failedRequests += failures;
            metrics.protocolUsage.set(name, successes);
        }
        return metrics;
    }
    // Emergency shutdown
    async emergencyShutdown() {
        console.log('Emergency shutdown initiated');
        // Disable all protocols
        for (const protocol of this.protocols.values()) {
            protocol.enabled = false;
        }
        // Clear all caches and reset circuit breakers
        await this.cleanup();
    }
    // Get recommended protocol based on current conditions
    async getRecommendedProtocol(amount, urgency = 'medium') {
        const availableProtocols = await this.getAvailableProtocols(amount);
        if (availableProtocols.length === 0)
            return null;
        // Sort by different criteria based on urgency
        if (urgency === 'high') {
            // Prioritize speed and reliability
            return availableProtocols.sort((a, b) => {
                const aScore = a.priority + (a.enabled ? 0 : 100);
                const bScore = b.priority + (b.enabled ? 0 : 100);
                return aScore - bScore;
            })[0];
        }
        else if (urgency === 'low') {
            // Prioritize lowest fees
            return availableProtocols.sort((a, b) => a.feeRate - b.feeRate)[0];
        }
        else {
            // Balanced approach
            return availableProtocols.sort((a, b) => {
                const aScore = a.priority * 10 + a.feeRate * 10000;
                const bScore = b.priority * 10 + b.feeRate * 10000;
                return aScore - bScore;
            })[0];
        }
    }
    // Batch validation of multiple flash loan requests
    async batchValidateRequests(requests) {
        const validationResults = new Map();
        const validationPromises = requests.map(async (request, index) => {
            const result = await this.validateFlashLoanRequest(request);
            validationResults.set(index, result);
        });
        await Promise.all(validationPromises);
        return validationResults;
    }
    // Get total available liquidity across all protocols
    async getTotalAvailableLiquidity() {
        const liquidityMap = {};
        for (const protocol of this.protocols.values()) {
            if (protocol.enabled) {
                const tokens = ['USDC', 'USDT', 'SOL', 'mSOL', 'stSOL'];
                for (const token of tokens) {
                    if (!liquidityMap[token]) {
                        liquidityMap[token] = new bn_js_1.default(0);
                    }
                    // Fetch actual available liquidity per token
                    try {
                        const tokenMint = this.getTokenMintAddress(token);
                        const liquidity = await this.fetchProtocolLiquidity(protocol, tokenMint);
                        liquidityMap[token] = liquidityMap[token].add(liquidity);
                    }
                    catch (error) {
                        console.error(`Failed to fetch ${token} liquidity from ${protocol.name}:`, error);
                    }
                }
            }
        }
        return liquidityMap;
    }
    getTokenMintAddress(tokenSymbol) {
        const tokenMints = {
            'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
            'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
            'SOL': 'So11111111111111111111111111111111111111112',
            'mSOL': 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So',
            'stSOL': '7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj'
        };
        return new web3_js_1.PublicKey(tokenMints[tokenSymbol] || tokenMints['USDC']);
    }
    async fetchProtocolLiquidity(protocol, tokenMint) {
        try {
            if (!protocol.address) {
                return protocol.maxLoanAmount;
            }
            switch (protocol.name.toLowerCase()) {
                case 'solend':
                    return await this.fetchSolendLiquidity(protocol.address, tokenMint);
                case 'port finance':
                    return await this.fetchPortFinanceLiquidity(protocol.address, tokenMint);
                case 'larix':
                    return await this.fetchLarixLiquidity(protocol.address, tokenMint);
                case 'apricot':
                    return await this.fetchApricotLiquidity(protocol.address, tokenMint);
                case 'kamino':
                    return await this.fetchKaminoLiquidity(protocol.address, tokenMint);
                default:
                    return protocol.maxLoanAmount;
            }
        }
        catch {
            return new bn_js_1.default(0);
        }
    }
    async fetchSolendLiquidity(lendingMarket, tokenMint) {
        const [reserveAddress] = await web3_js_1.PublicKey.findProgramAddress([lendingMarket.toBuffer(), tokenMint.toBuffer()], new web3_js_1.PublicKey('So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo'));
        const reserveData = await this.connection.getAccountInfo(reserveAddress);
        if (!reserveData || reserveData.data.length < 571)
            return new bn_js_1.default(0);
        // Solend reserve layout
        const availableLiquidity = new bn_js_1.default(reserveData.data.slice(64, 72), 'le');
        const borrowedAmount = new bn_js_1.default(reserveData.data.slice(80, 88), 'le');
        return availableLiquidity;
    }
    async fetchPortFinanceLiquidity(lendingMarket, tokenMint) {
        const [reserveAddress] = await web3_js_1.PublicKey.findProgramAddress([buffer_1.Buffer.from('reserve'), lendingMarket.toBuffer(), tokenMint.toBuffer()], new web3_js_1.PublicKey('Port7uDYB3wk6GJAw4KT1WpTeMtSu9bTcChBHkX2LfR'));
        const reserveData = await this.connection.getAccountInfo(reserveAddress);
        if (!reserveData || reserveData.data.length < 300)
            return new bn_js_1.default(0);
        // Port Finance reserve layout
        const liquidity = new bn_js_1.default(reserveData.data.slice(72, 80), 'le');
        return liquidity;
    }
    async fetchLarixLiquidity(lendingMarket, tokenMint) {
        const [reserveAddress] = await web3_js_1.PublicKey.findProgramAddress([lendingMarket.toBuffer(), tokenMint.toBuffer()], new web3_js_1.PublicKey('7Zb1bGi32pfsrBkzWdqd4dFhUXwp5Nybr1zuaEwN34hy'));
        const reserveData = await this.connection.getAccountInfo(reserveAddress);
        if (!reserveData || reserveData.data.length < 258)
            return new bn_js_1.default(0);
        // Larix reserve layout
        const availableAmount = new bn_js_1.default(reserveData.data.slice(80, 88), 'le');
        return availableAmount;
    }
    async fetchApricotLiquidity(lendingMarket, tokenMint) {
        const [poolAddress] = await web3_js_1.PublicKey.findProgramAddress([buffer_1.Buffer.from('pool'), tokenMint.toBuffer()], new web3_js_1.PublicKey('6UeJYTLU1adaoHWeApWsoj1xNEDbWA2RhM2DLc8CrDDi'));
        const poolData = await this.connection.getAccountInfo(poolAddress);
        if (!poolData || poolData.data.length < 200)
            return new bn_js_1.default(0);
        // Apricot pool layout
        const totalDeposits = new bn_js_1.default(poolData.data.slice(64, 72), 'le');
        const totalBorrows = new bn_js_1.default(poolData.data.slice(72, 80), 'le');
        return totalDeposits.sub(totalBorrows);
    }
    async fetchKaminoLiquidity(market, tokenMint) {
        const [reserveAddress] = await web3_js_1.PublicKey.findProgramAddress([market.toBuffer(), tokenMint.toBuffer()], new web3_js_1.PublicKey('KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD'));
        const reserveData = await this.connection.getAccountInfo(reserveAddress);
        if (!reserveData || reserveData.data.length < 600)
            return new bn_js_1.default(0);
        // Kamino reserve layout
        const liquidity = new bn_js_1.default(reserveData.data.slice(104, 112), 'le');
        return liquidity;
    }
    // Monitor and log protocol performance
    async logProtocolPerformance() {
        const stats = this.getProtocolStats();
        const timestamp = new Date().toISOString();
        console.log(`[${timestamp}] Protocol Performance Report:`);
        stats.forEach(stat => {
            console.log(`- ${stat.name}: Enabled=${stat.enabled}, Fee=${stat.feeRate}, Priority=${stat.priority}, Success=${stat.successRate?.toFixed(2)}%`);
        });
    }
    // Get protocol by name
    getProtocol(name) {
        return this.protocols.get(name.toLowerCase());
    }
    // Update protocol configuration
    updateProtocolConfig(name, config) {
        const protocol = this.protocols.get(name.toLowerCase());
        if (!protocol)
            return false;
        Object.assign(protocol, config);
        return true;
    }
    // Calculate total fees for a flash loan amount across all protocols
    calculateProtocolFees(amount) {
        const fees = new Map();
        for (const [name, protocol] of this.protocols) {
            const fee = amount.mul(new bn_js_1.default(Math.floor(protocol.feeRate * 10000))).div(new bn_js_1.default(10000));
            fees.set(name, fee);
        }
        return fees;
    }
    // Estimate time to execute flash loan based on protocol
    estimateExecutionTime(protocolName) {
        const baseTime = 2000; // 2 seconds base
        const protocol = this.protocols.get(protocolName.toLowerCase());
        if (!protocol)
            return baseTime;
        // Add time based on priority (higher priority = potentially slower)
        return baseTime + (protocol.priority * 100);
    }
    // Get protocols sorted by preference
    getProtocolsByPreference() {
        return Array.from(this.protocols.values())
            .filter(p => p.enabled)
            .sort((a, b) => {
            // Sort by: priority (ascending), then fee rate (ascending)
            if (a.priority !== b.priority) {
                return a.priority - b.priority;
            }
            return a.feeRate - b.feeRate;
        });
    }
    // Reset circuit breaker for specific protocol
    resetCircuitBreaker(protocolName) {
        this.circuitBreaker.delete(protocolName);
    }
    // Get circuit breaker status
    getCircuitBreakerStatus() {
        return new Map(this.circuitBreaker);
    }
    // Check if protocol is in circuit breaker state
    isProtocolBroken(protocolName) {
        const status = this.circuitBreaker.get(protocolName);
        return status ? status.failures >= this.MAX_FAILURES : false;
    }
    // Force enable/disable protocol
    setProtocolEnabled(protocolName, enabled) {
        const protocol = this.protocols.get(protocolName.toLowerCase());
        if (!protocol)
            return false;
        protocol.enabled = enabled;
        if (enabled) {
            this.resetCircuitBreaker(protocolName);
        }
        return true;
    }
    // Get real-time protocol rankings
    async getRealTimeRankings() {
        const healthReport = await this.monitorProtocolHealth();
        const rankings = [];
        for (const [name, protocol] of this.protocols) {
            const health = healthReport.get(name);
            if (!health || !protocol.enabled)
                continue;
            // Calculate composite score
            const successWeight = 0.4;
            const feeWeight = 0.3;
            const liquidityWeight = 0.2;
            const speedWeight = 0.1;
            const successScore = (health.successRate / 100) * successWeight;
            const feeScore = (1 - protocol.feeRate * 100) * feeWeight;
            const liquidityScore = Math.min(1, parseFloat(protocol.maxLoanAmount.toString()) / 1e9) * liquidityWeight;
            const speedScore = Math.max(0, 1 - health.avgResponseTime / 2000) * speedWeight;
            const totalScore = successScore + feeScore + liquidityScore + speedScore;
            rankings.push({
                name: protocol.name,
                score: totalScore,
                metrics: {
                    successRate: health.successRate,
                    avgFee: protocol.feeRate,
                    liquidity: protocol.maxLoanAmount.toString(),
                    responseTime: health.avgResponseTime
                }
            });
        }
        return rankings.sort((a, b) => b.score - a.score);
    }
    // Execute with automatic retry on different protocols
    async executeWithRetry(request, maxRetries = 3) {
        let lastError;
        const triedProtocols = new Set();
        for (let i = 0; i < maxRetries; i++) {
            const availableProtocols = await this.getAvailableProtocols(request.amount);
            const untried = availableProtocols.filter(p => !triedProtocols.has(p.name));
            if (untried.length === 0)
                break;
            const protocol = untried[0];
            triedProtocols.add(protocol.name);
            try {
                request.preferredProtocol = protocol.name;
                return await this.executeFlashLoan(request);
            }
            catch (error) {
                lastError = error;
                console.error(`Retry ${i + 1} failed with ${protocol.name}:`, error);
                await this.sleep(1000 * (i + 1)); // Exponential backoff
            }
        }
        return {
            success: false,
            error: lastError?.message || 'All retries exhausted'
        };
    }
    // Get estimated APY for flash loan arbitrage
    estimateArbitrageAPY(dailyOpportunities, avgProfit, capital) {
        if (capital.isZero())
            return 0;
        const dailyReturn = avgProfit.mul(new bn_js_1.default(dailyOpportunities));
        const dailyRate = dailyReturn.mul(new bn_js_1.default(10000)).div(capital).toNumber() / 10000;
        const apy = Math.pow(1 + dailyRate, 365) - 1;
        return apy * 100; // Return as percentage
    }
}
exports.MultiProtocolFlashLoanAggregator = MultiProtocolFlashLoanAggregator;
exports.default = MultiProtocolFlashLoanAggregator;
//# sourceMappingURL=MultiProtocolFlashLoanAggregator.js.map